
var async = require('async');
var mysql = require('mysql');
var _ = require('underscore');
var noop = function(){};
var logPrefix = '[nodebb-plugin-import-kunena]';

// add converter for bbcode
var path = require('path');
var converter = require( path.resolve( __dirname, "./converter.js" ) );


(function(Exporter) {

	Exporter.setup = function(config, callback) {
		Exporter.log('setup');

		// mysql db only config
		// extract them from the configs passed by the nodebb-plugin-import adapter
		var _config = {
			host: config.dbhost || config.host || 'localhost',
			user: config.dbuser || config.user || 'root',
			password: config.dbpass || config.pass || config.password || '',
			port: config.dbport || config.port || 3306,
			database: config.dbname || config.name || config.database || 'ubb'
		};

		Exporter.config(_config);
		Exporter.config('prefix', config.prefix || config.tablePrefix || '');

		Exporter.connection = mysql.createConnection(_config);
		Exporter.connection.connect();

		callback(null, Exporter.config());
	};

	Exporter.getUsers = function(callback) {
		return Exporter.getPaginatedUsers(0, -1, callback);
	};
	Exporter.getPaginatedUsers = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
				+ prefix + 'kunena_users.userid as _uid, '
				+ prefix + 'users.username as _username, '
				+ prefix + 'users.name as _alternativeUsername, '
				+ prefix + 'users.email as _registrationEmail, '
				+ prefix + 'kunena_users.rank as _level, '
				+ prefix + 'users.registerDate as _joindate, '
				+ prefix + 'kunena_users.banned as _banned, '
				+ prefix + 'users.email as _email, '
				+ prefix + 'kunena_users.signature as _signature, '
				+ prefix + 'kunena_users.websiteurl as _website, '
				+ prefix + 'kunena_users.status_text as _occupation, '
				+ prefix + 'kunena_users.location as _location, '
				+ prefix + 'kunena_users.avatar as _picture, '
				+ prefix + 'kunena_users.status as _badge, '
				+ prefix + 'kunena_users.thankyou as _reputation, '
				+ prefix + 'kunena_users.view as _profileviews, '
				+ prefix + 'kunena_users.birthdate as _birthday, '
				+ prefix + 'users.block as _banned, '
				+ prefix + 'kunena_users.group_id as _gid '

				+ 'FROM ' + prefix + 'kunena_users '
				+ 'JOIN ' + prefix + 'users ON ' + prefix + 'kunena_users.userid = ' + prefix + 'users.id '
				//+ 'LEFT JOIN ' + prefix + 'BANNED_USERS ON ' + prefix + 'BANNED_USERS.USER_ID = ' + prefix + 'USERS.USER_ID '
				//+ 'LEFT JOIN ' + prefix + 'USER_GROUPS ON ' + prefix + 'USER_GROUPS.USER_ID = ' + prefix + 'USERS.USER_ID '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row) {
						// from unix timestamp (s) to JS timestamp (ms)
						row._joindate = ((row._joindate || 0) * 1000) || startms;

						// lower case the email for consistency
						row._email = (row._email || '').toLowerCase();

						// I don't know about you about I noticed a lot my users have incomplete urls, urls like: http://
						row._picture = Exporter.validateUrl(row._picture);
						row._website = Exporter.validateUrl(row._website);

						row._banned = row._banned ? 1 : 0;

						if (row._gid) {
							row._groups = [row._gid];
						}
						delete row._gid;


						map[row._uid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.getGroups = function(callback) {
		return Exporter.getPaginatedGroups(0, -1, callback);
	};

	Exporter.getPaginatedGroups = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
				+ prefix + 'usergroups.id as _gid, '
				+ prefix + 'usergroups.title as _name, '
				+ prefix + 'user_usergroup_map.users_id AS _ownerUid '
				+ 'FROM ' + prefix + 'user_usergroup_map '
				+ 'JOIN ' + prefix + 'usergroups ON ' + prefix + 'user_usergroup_map.group_id=' + prefix + 'usergroups.id '
				+ 'GROUP BY ' + prefix + 'user_usergroup_map.group_id '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}
					//normalize here
					var map = {};
					rows.forEach(function(row) {
						map[row._uid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.getCategories = function(callback) {
		return Exporter.getPaginatedCategories(0, -1, callback);
	};
	Exporter.getPaginatedCategories = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
				+ prefix + 'kunena_categories.id as _cid, '
				+ prefix + 'kunena_categories.name as _name, '
				+ prefix + 'kunena_categories.parent_id as _parentCid, '
				+ prefix + 'kunena_categories.ordering as _order, '
				+ prefix + 'kunena_categories.description as _description '
				//+ prefix + 'FORUMS.FORUM_CREATED_ON as _timestamp '
				+ 'FROM ' + prefix + 'kunena_categories '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row) {
						row._name = row._name || 'Untitled Category '
						row._description = row._description || 'No decsciption available';
						row._timestamp = ((row._timestamp || 0) * 1000) || startms;

						map[row._cid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.getTopics = function(callback) {
		return Exporter.getPaginatedTopics(0, -1, callback);
	};
	Exporter.getPaginatedTopics = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query =
				'SELECT '
				+ prefix + 'kunena_topics.id as _tid, '
				+ prefix + 'kunena_topics.subject as _title, '
				+ prefix + 'kunena_topics.category_id as _cid, '
				+ prefix + 'kunena_topics.first_post_userid as _uid, '
				+ prefix + 'kunena_topics.first_post_guest_name as _guest, '
				+ prefix + 'kunena_topics.first_post_message as _content, '
				+ prefix + 'kunena_topics.hits as _viewcount, '
				+ prefix + 'kunena_topics.first_post_time as _timestamp, '
				//+ prefix + 'TOPICS.TOPIC_IS_STICKY as _pinned, '
				+ prefix + 'kunena_topics.last_post_time as _edited '
				//+ prefix + 'POSTS.POST_POSTER_IP as _ip '
				+ 'FROM ' + prefix + 'kunena_topics '
				//+ 'JOIN ' + prefix + 'POSTS ON ' + prefix + 'POSTS.TOPIC_ID=' + prefix + 'TOPICS.TOPIC_ID '
				//+ 'AND ' + prefix + 'POSTS.POST_PARENT_ID=0 '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};

					rows.forEach(function(row) {
						row._title = row._title ? row._title[0].toUpperCase() + row._title.substr(1) : 'Untitled';
						row._timestamp = ((row._timestamp || 0) * 1000) || startms;
						row._edited = row._edited ? row._edited * 1000 : row._edited;

						map[row._tid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.getPosts = function(callback) {
		return Exporter.getPaginatedPosts(0, -1, callback);
	};
	Exporter.getPaginatedPosts = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query =
				'SELECT '
				+ prefix + 'kunena_messages.id as _pid, '
				+ prefix + 'kunena_messages.userid as _uid, '
				+ prefix + 'kunena_messages.thread as _tid, '
				+ prefix + 'kunena_messages_text.message as _content, '
				+ prefix + 'kunena_messages.time as _timestamp, '
				+ prefix + 'kunena_messages.parent as _toPid, '
				+ prefix + 'kunena_messages.locked as _locked, '
				//+ prefix + 'POST_LAST_EDITED_TIME as _edited, '
				+ prefix + 'kunena_messages.ip as _ip '


				+ 'FROM ' + prefix + 'kunena_messages '
				+ 'JOIN ' + prefix + 'kunena_messages_text ON ' + prefix + 'kunena_messages.id=' + prefix + 'kunena_messages_text.mesid '

					// this post cannot be a its topic's main post, it MUST be a reply-post
					// see https://github.com/akhoury/nodebb-plugin-import#important-note-on-topics-and-posts
				+ 'WHERE '+ prefix + 'kunena_messages.parent	 > 0 '
				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row) {
            //convert bbcode
            // if(row._content){
            //   var postContent = converter.parse(row._content, function(first_param, postContent){
            //     return postContent;
            //   });
            // }


						row._content = row._content || '';
						row._timestamp = ((row._timestamp || 0) * 1000) || startms;
						row._edited = row._edited ? row._edited * 1000 : row._edited;
						map[row._pid] = row;
					});

					callback(null, map);
				});
	};


	// todo: possible memory issues
	function getConversations (callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix');
		var startms = +new Date();

		// var query = 'SELECT '
		// 		+ prefix + 'PRIVATE_MESSAGE_USERS.TOPIC_ID as _cvid, '
		// 		+ prefix + 'PRIVATE_MESSAGE_USERS.USER_ID as _uid1, '
		// 		+ prefix + 'PRIVATE_MESSAGE_POSTS.USER_ID as _uid2 '
		// 		+ 'FROM ' + prefix + 'PRIVATE_MESSAGE_USERS '
		// 		+ 'JOIN ' + prefix + 'PRIVATE_MESSAGE_POSTS '
		// 		+ 'ON ' + prefix + 'PRIVATE_MESSAGE_POSTS.TOPIC_ID = ' + prefix + 'PRIVATE_MESSAGE_USERS.TOPIC_ID '
		// 		+ 'AND ' + prefix + 'PRIVATE_MESSAGE_POSTS.USER_ID != ' + prefix + 'PRIVATE_MESSAGE_USERS.USER_ID '

		var query='';


		var parse = function(v) { return parseInt(v, 10); };

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row) {
						if (!row._uid1 || !row._uid2) {
							return;
						}
						row._uids = {};
						row._uids[row._uid1] = row._uid2;
						row._uids[row._uid2] = row._uid1

						map[row._cvid] = row;
					});

					callback(null, map);
				});

	}

	Exporter.getMessages = function(callback) {
		return Exporter.getPaginatedMessages(0, -1, callback);
		//return '';
	};
	Exporter.getPaginatedMessages = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		if (!Exporter.connection) {
			Exporter.setup(Exporter.config());
		}

		var prefix = Exporter.config('prefix');
		var startms = +new Date();

		// var query = 'SELECT '
		// 		+ prefix + 'PRIVATE_MESSAGE_POSTS.POST_ID as _mid, '
		// 		+ prefix + 'PRIVATE_MESSAGE_POSTS.POST_BODY as _content, '
		// 		+ prefix + 'PRIVATE_MESSAGE_POSTS.USER_ID as _fromuid, '
		// 		+ prefix + 'PRIVATE_MESSAGE_POSTS.TOPIC_ID as _cvid, '
		// 		+ prefix + 'PRIVATE_MESSAGE_POSTS.POST_TIME as _timestamp '

		// 		+ 'FROM ' + prefix + 'PRIVATE_MESSAGE_POSTS '
		// 		+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

		var query = ' ';

		getConversations(function(err, conversations) {
			if (err) {
				return callback(err);
			}

			Exporter.connection.query(query,
					function(err, rows) {
						if (err) {
							Exporter.error(err);
							return callback(err);
						}

						//normalize here
						var map = {};
						rows.forEach(function(row) {

							var conversation = conversations[row._cvid];
							if (!conversation) {
								return;
							}

							row._touid = conversation._uids[row._fromuid];
							if (!row._touid) {
								return;
							}

							row._content = row._content || '';
							row._timestamp = ((row._timestamp || 0) * 1000) || startms;

							delete row._cvid;

							map[row._mid] = row;
						});

						callback(null, map);
					});
		});
	};

	Exporter.teardown = function(callback) {
		Exporter.log('teardown');
		Exporter.connection.end();

		Exporter.log('Done');
		callback();
	};

	Exporter.testrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			function(next) {
				Exporter.getUsers(next);
			},
			function(next) {
				Exporter.getGroups(next);
			},
			function(next) {
				Exporter.getCategories(next);
			},
			function(next) {
				Exporter.getTopics(next);
			},
			function(next) {
				Exporter.getPosts(next);
			},
			// function(next) {
			// 	//Exporter.getMessages(next);
			// },
			function(next) {
				Exporter.teardown(next);
			}
		], callback);
	};

	Exporter.paginatedTestrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			function(next) {
				Exporter.getPaginatedUsers(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedCategories(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedTopics(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedPosts(1001, 2000, next);
			},
			function(next) {
				Exporter.teardown(next);
			}
		], callback);
	};

	Exporter.warn = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.warn.apply(console, args);
	};

	Exporter.log = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.log.apply(console, args);
	};

	Exporter.error = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.error.apply(console, args);
	};

	Exporter.config = function(config, val) {
		if (config != null) {
			if (typeof config === 'object') {
				Exporter._config = config;
			} else if (typeof config === 'string') {
				if (val != null) {
					Exporter._config = Exporter._config || {};
					Exporter._config[config] = val;
				}
				return Exporter._config[config];
			}
		}
		return Exporter._config;
	};

	// from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
	Exporter.validateUrl = function(url) {
		var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
		return url && url.length < 2083 && url.match(pattern) ? url : '';
	};

	Exporter.truncateStr = function(str, len) {
		if (typeof str != 'string') return str;
		len = _.isNumber(len) && len > 3 ? len : 20;
		return str.length <= len ? str : str.substr(0, len - 3) + '...';
	};

	Exporter.whichIsFalsy = function(arr) {
		for (var i = 0; i < arr.length; i++) {
			if (!arr[i])
				return i;
		}
		return null;
	};

})(module.exports);
