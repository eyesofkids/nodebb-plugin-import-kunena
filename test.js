var fs = require('fs-extra');

require('./index').testrun({
    dbhost: 'localhost',
    dbport: 3306,
    dbname: 'kunena',
    dbuser: 'kunena',
    dbpass: 'password',

    tablePrefix: 'prefix_'
}, function(err, results) {
    results.forEach(function(result, i) {
		console.log(i, result && Object.keys(result).length);
	});
	 fs.writeFileSync('./test.json', JSON.stringify(results, undefined, 2));
});
