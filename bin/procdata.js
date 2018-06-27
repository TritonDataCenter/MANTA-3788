#!/usr/bin/env node

/*
 * procdata.js: process a data file.  See MANTA-3788 for context.
 */

var mod_assertplus = require('assert-plus');
var mod_cmdutil = require('cmdutil');
var mod_fs = require('fs');
var mod_jsprim = require('jsprim');
var mod_lstream = require('lstream');
var mod_stream = require('stream');
var mod_util = require('util');
var mod_vasync = require('vasync');
var mod_vstream = require('vstream');
var VError = require('verror');

/* maximum number of relations supported (for protecting against huge input) */
var NRELATIONS_MAX = 100;
/* maximum size of PostgreSQL table data files */
var PG_FILE_MAX = 1024 * 1024 * 1024;

function main()
{
	var argv;
	var relmapfile, datafile;
	var relnames;

	mod_cmdutil.configure({
	    'synopses': [ 'REL_MAP_FILE DATA_FILE' ],
	    'usageMessage': 'process a single MANTA-3788 data file'
	});

	argv = process.argv.slice(2);
	if (argv.length < 2) {
		mod_cmdutil.usage();
	}

	relmapfile = argv[0];
	datafile = argv[1];
	relnames = new RelNameCollector({});

	mod_vasync.waterfall([
	    function loadRelMapFile(callback) {
		var fstream, lstream, parser;

		fstream = mod_vstream.wrapStream(
		    mod_fs.createReadStream(relmapfile), relmapfile);
		lstream = mod_vstream.wrapStream(
		    new mod_lstream(), 'line separator');
		parser = new RelNameParser({});

		fstream.pipe(lstream);
		lstream.pipe(parser);
		parser.pipe(relnames);

		parser.on('warn', function (context, kind, error) {
			/* XXX tear down pipeline */
			callback(new VError(error,
			    'fatal error parsing relation mapping'));
		});

		relnames.on('error', function (error) {
			callback(new VError(error,
			    'fatal error processing relation mapping'));
		});

		relnames.on('warn', function (context, kind, error) {
			/* XXX tear down pipeline */
			callback(new VError(error,
			    'fatal error processing relation mapping'));
		});

		parser.on('finish', callback);
	    },

	    function processData(callback) {
		var fstream, lstream, parser, serializer;

		fstream = mod_vstream.wrapStream(
		    mod_fs.createReadStream(datafile), datafile);
		lstream = mod_vstream.wrapTransform(
		    new mod_lstream(), 'line separator');
		parser = new DataFileParser({
		    'relnames': relnames
		});
		serializer = new JsonSerializer({});

		fstream.pipe(lstream);
		lstream.pipe(parser);

		/* TODO */
		parser.pipe(serializer);
		serializer.pipe(process.stdout);

		parser.on('warn', function (context, kind, error) {
			mod_cmdutil.warn('%s: %s', context !== null ?
			    context.label() : 'unknown context',
			    error.message);
		});

		parser.on('finish', callback);
	    }
	], function (err) {
		if (err) {
			mod_cmdutil.fail(err);
		}

		/* TODO dump counters */
	});
}

/*
 * RelNameParser is an object-mode transform string that translates lines from a
 * relation mapping file into objects describing the mapping.  The input file is
 * expected to look like this:
 *
 *     $relation_name|$relid
 *
 * This would be the output of a command like:
 *
 *     psql -t -P format=unaligned -c \
 *         "SELECT relname, relfilenode FROM pg_class where relname like \
 *         'manta%' order by relfilenode asc;" moray;
 */
function RelNameParser(options)
{
	var streamOptions;

	mod_assertplus.object(options, 'options');
	streamOptions = mod_jsprim.mergeObjects(options.streamOptions,
	    { 'objectMode': true }, { 'highWaterMark': 8 });
	mod_stream.Transform.call(this, streamOptions);
	mod_vstream.wrapStream(this, this.constructor.name);
}

mod_util.inherits(RelNameParser, mod_stream.Transform);

RelNameParser.prototype._transform = function (chunk, _, callback)
{
	var parts;

	mod_assertplus.string(chunk, 'chunk');
	mod_assertplus.equal(-1, chunk.indexOf('\n'),
	    'missing newline parser ahead of RelNameParser');

	/* We always complete immediately. */
	setImmediate(callback);

	parts = chunk.split('|');
	if (parts.length != 2) {
		this.vsWarn(new VError('expected exactly one "|"'), 'ngarbled');
		return;
	}

	this.push({ 'relname': parts[0], 'relid': parts[1] });
};

/*
 * RelNameCollector receives objects emitted by RelNameParser and combines them
 * into a single object.
 */
function RelNameCollector(options)
{
	var streamOptions;

	mod_assertplus.object(options, 'options');
	streamOptions = mod_jsprim.mergeObjects(options.streamOptions,
	    { 'objectMode': true }, { 'highWaterMark': 8 });
	mod_stream.Writable.call(this, streamOptions);
	mod_vstream.wrapStream(this, this.constructor.name);

	this.rnc_relname_to_relid = {};
	this.rnc_relid_to_relname = {};
	this.rnc_count = 0;
	this.rnc_max = NRELATIONS_MAX;
	this.rnc_overflow = false;
}

mod_util.inherits(RelNameCollector, mod_stream.Writable);

RelNameCollector.prototype._write = function (chunk, _, callback)
{
	mod_assertplus.object(chunk, 'chunk');
	mod_assertplus.string(chunk.relname, 'chunk.relname');
	mod_assertplus.string(chunk.relid, 'chunk.relid');

	/* Protect against bad input causing us to run out of memory. */
	mod_assertplus.ok(!this.rnc_overflow,
	    'given more data after emitting error');

	if (++this.rnc_count == this.rnc_max) {
		this.rnc_overflow = true;
		setImmediate(callback, new VError(
		    'exceeded max of %d relation names', this.rnc_max));
		return;
	}

	/* We always complete immediately. */
	setImmediate(callback);

	if (mod_jsprim.hasKey(this.rnc_relname_to_relid, chunk.relname)) {
		this.vsWarn(new VError('duplicate relation name: "%s" ' +
		    '(previously relid "%s", now "%s")', chunk.relname,
		    this.rnc_relname_to_relid[chunk.relname],
		    chunk.relid), 'nduprelname');
		return;
	}

	if (mod_jsprim.hasKey(this.rnc_relid_to_relname, chunk.relid)) {
		this.vsWarn(new VError('duplicate relid: "%s" ' +
		    '(previously relation name "%s", now "%s")',
		    chunk.relid, this.rnc_relid_to_relname[chunk.relid],
		    chunk.relname), 'nduprelid');
		return;
	}

	this.rnc_relname_to_relid[chunk.relname] = chunk.relid;
	this.rnc_relid_to_relname[chunk.relid] = chunk.relname;
};

RelNameCollector.prototype.relnameForRelid = function (relid)
{
	return (mod_jsprim.hasKey(this.rnc_relid_to_relname, relid) ?
	    this.rnc_relid_to_relname[relid] : null);
};

RelNameCollector.prototype.relidForRelname = function (relname)
{
	return (mod_jsprim.hasKey(this.rnc_relname_to_relid, relname) ?
	    this.rnc_relname_to_relid[relname] : null);
};


/*
 * Parses a data file from MANTA-3788-v1.d.  All lines start with:
 *
 *     HRTIMESTAMP_SECONDS.HRTIMESTAMP_NSECONDS YYYY MMM DD HH:MM:ZZ.NANOSEC
 *
 * The first line then reports "begin tracing".
 * Subsequent lines have:
 *
 *     OPNAME offset OFFSET size SIZE file FILENAME
 *
 * where OPNAME is either "read" or "write", OFFSET and SIZE are hex integers,
 * and filename generally looks like:
 *
 *     /zones/ZONENAME/root/manatee/pg/data/base/DBID/RELID.SUFFIX
 *
 * For each input line matching this pattern, this stream emits an object
 * describing the line.
 */
function DataFileParser(args)
{
	var streamOptions;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.relnames, 'args.relnames');
	streamOptions = mod_jsprim.mergeObjects(args.streamOptions,
	    { 'objectMode': true }, { 'highWaterMark': 8 });
	mod_stream.Transform.call(this, streamOptions);
	mod_vstream.wrapStream(this, this.constructor.name);

	this.dfp_relnames = args.relnames;
	this.dfp_first = true;
}

mod_util.inherits(DataFileParser, mod_stream.Transform);

DataFileParser.prototype._transform = function (chunk, _, callback)
{
	var parts;
	var hrtimeparts, hrtime, hrtmillis;
	var datestr, when;
	var messageparts, rv;

	mod_assertplus.string(chunk, 'chunk');
	mod_assertplus.equal(-1, chunk.indexOf('\n'),
	    'missing newline parser ahead of FileNameParser');

	/* We always complete immediately. */
	setImmediate(callback);

	parts = chunk.split(/\s+/);
	if (parts.length < 6) {
		this.vsWarn(new VError('expected at least 6 parts'), 'nshort');
		return;
	}

	/*
	 * Parse the high-resolution timestamp.
	 */
	hrtimeparts = parts[0].split('.');
	if (hrtimeparts.length != 2) {
		this.vsWarn(new VError('garbled hrtime'), 'nbadhrtime');
		return;
	}

	hrtime = [
	    mod_jsprim.parseInteger(hrtimeparts[0]),
	    mod_jsprim.parseInteger(hrtimeparts[1])
	];
	if (hrtime[0] instanceof Error) {
		this.vsWarn(new VError(hrtime[0], 'bad hrtime (second part)'),
		    'nbadhrtime');
		return;
	}
	if (hrtime[1] instanceof Error) {
		this.vsWarn(new VError(hrtime[0], 'bad hrtime (nanosec part)'),
		    'nbadhrtime');
		return;
	}

	hrtmillis = mod_jsprim.hrtimeMillisec(hrtime);

	/*
	 * Parse the date string.  (We should have put a 'Z' on the end of the
	 * time in the input format to indicate the time zone is UTC, but here
	 * we are.)
	 */
	datestr = parts.slice(1, 5).join(' ');
	when = new Date(datestr + 'Z');
	if (isNaN(when.getTime())) {
		this.vsWarn(new VError('unsupported date'), 'nbaddate');
		return;
	}

	messageparts = parts.slice(5);
	if (this.dfp_first) {
		if (messageparts.length != 2 || messageparts[0] != 'begin' ||
		    messageparts[1] != 'tracing') {
			this.vsWarn(new VError('bad first line'), 'nbadfirst');
			return;
		}

		this.dfp_first = false;
		return;
	}

	if (messageparts.length != 7 ||
	    (messageparts[0] != 'read' && messageparts[0] != 'write') ||
	    messageparts[1] != 'offset' ||
	    messageparts[3] != 'size' ||
	    messageparts[5] != 'file') {
		this.vsWarn(new VError('malformed message'), 'nbadmessage');
		return;
	}

	rv = {
	    /* hrtimestamp, in milliseconds */
	    'hrtimeMillis': hrtmillis,

	    /* wall timestamp, as a Date */
	    'walltime': when,

	    /* operation type ("read" or "write") */
	    'optype': messageparts[0],

	    /* boolean indicating whether this is a read */
	    'isRead': messageparts[0] == 'read',

	    /* size of the read or write operation */
	    'size': null,

	    /* offset into the file */
	    'offset': null,

	    /* information derived from the path */

	    /* full path */
	    'fullPath': messageparts[6],
	    /* zone name */
	    'zonename': null,
	    /* database id */
	    'dbid': null,
	    /* relation id */
	    'relid': null,

	    /* relation name (mapped from relation id, if possible) */
	    'relname': null,
	    /* which of the relation's files this is */
	    'filenum': null,
	    /* path to the file, relative to the root of the database */
	    'file': null,
	    /* recommended label for the file */
	    'fileLabel': null,

	    /* logical offset into the whole relation (TODO) */
	    'logicalOffset': null
	};

	/*
	 * Parse the size and offset values, which are generally hex numbers.
	 */
	rv.size = mod_jsprim.parseInteger(
	    messageparts[4], { 'allowPrefix': true });
	if (rv.size instanceof Error) {
		this.vsWarn(new VError(rv.size, 'bad size'), 'nbadsize');
		return;
	}

	rv.offset = mod_jsprim.parseInteger(messageparts[2],
	    { 'allowPrefix': true });
	if (rv.size instanceof Error) {
		this.vsWarn(new VError(rv.size, 'bad offset'), 'nbadoffset');
		return;
	}

	/*
	 * Parse out relevant bits of the path.  At this point, we have no more
	 * errors.
	 */
	this.vsCounterBump('nops');
	parts = messageparts[6].split(/\/+/);
	mod_assertplus.ok(parts.length > 0);
	rv.file = parts[parts.length - 1];

	/* Eat the leading empty component. */
	if (parts.length > 0 && parts[0] === '') {
		parts.shift();
	}

	/* If present, eat the zone root, and record the zone name. */
	if (parts.length > 1 && parts[0] == 'zones' && parts[2] == 'root') {
		rv.zonename = parts[1];
		parts.shift();
		parts.shift();
		parts.shift();
	}

	/*
	 * If present, eat the path to the database.
	 */
	if (parts.length >= 3 && parts[0] == 'manatee' && parts[1] == 'pg' &&
	    parts[2] == 'data') {
		parts.shift();
		parts.shift();
		parts.shift();
	}

	/*
	 * At this point, if we've got nothing better, we'll end up reporting
	 * the base name (relative to the Manatee data directory).
	 */
	rv.fileLabel = parts.join('/');
	if (parts.length != 3 || parts[0] != 'base') {
		this.vsCounterBump('nops_nobase');
		this.push(rv);
		return;
	}

	/*
	 * If possible, parse out the dbid, relation id, and map that back to
	 * the relation name.
	 * XXX This doesn't work for the first file in the relation, nor the _vm
	 * or _fsm files (maybe those are okay though).
	 */
	rv.dbid = parts[1];
	parts = parts[2].split('.');
	if (parts.length != 2) {
		this.vsCounterBump('nops_nosuffix');
		this.push(rv);
		return;
	}

	rv.relid = parts[0];
	rv.relname = this.dfp_relnames.relnameForRelid(parts[0]);
	if (rv.relname === null) {
		this.vsCounterBump('nops_norelname');
		return;
	}
	rv.filenum = mod_jsprim.parseInteger(parts[1]);
	if (rv.filenum instanceof Error) {
		rv.filenum = null;
		this.vsCounterBump('nops_badfilenum');
	}

	rv.fileLabel = rv.relname + '.' + parts[1];
	rv.logicalOffset = rv.filenum * PG_FILE_MAX + rv.offset;
	this.push(rv);
};


/*
 * Transform string that takes JS objects and turns them back to JSON.
 */
function JsonSerializer(options)
{
	var streamOptions;

	mod_assertplus.object(options, 'options');
	streamOptions = mod_jsprim.mergeObjects(options.streamOptions,
	    { 'objectMode': true }, { 'highWaterMark': 1024 });
	mod_stream.Transform.call(this, streamOptions);
	mod_vstream.wrapTransform(this, this.constructor.name);
}

mod_util.inherits(JsonSerializer, mod_stream.Transform);

JsonSerializer.prototype._transform = function (chunk, _, callback)
{
	var str;

	mod_assertplus.object(chunk, 'chunk');

	/* We always complete immediately without a fatal error. */
	setImmediate(callback);

	try {
		str = JSON.stringify(chunk) + '\n';
	} catch (ex) {
		/* TODO this should be reported to the user prominently */
		this.vsWarn(new VError(ex, 'failed to serialize JSON'),
		    'nerr_jsonserializer');
		return;
	}

	this.push(str);
};

main();
