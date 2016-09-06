var fs = require('fs-extra');

require('./index').testrun({
    dbhost: 'joomla.dev',
    dbport: 3306,
    dbname: 'kunena_import_test',
    dbuser: 'kunena',
    dbpass: '12345678',

    tablePrefix: 'amnp8_'
}, function(err, results) {
    results.forEach(function(result, i) {
		console.log(i, result && Object.keys(result).length);
	});
	 fs.writeFileSync('./results.json', JSON.stringify(results, undefined, 2));
});
