var express         = require('express');
var path            = require('path');
var config          = require('./libs/config');
var log             = require('./libs/log')(module);
var registry        = require('./libs/registry');
var RegistryModel   = require('./libs/registry').RegistryModel;
var app = express();

app.use(express.favicon());
app.use(express.logger('dev'));
//app.use(express.bodyParser());
app.use(require('connect').bodyParser());
app.use(express.methodOverride());
app.use(app.router);
app.use(express.static(path.join(__dirname, "public")));
app.use(express.errorHandler());

app.use(function(req, res, next){
    res.status(404);
    log.debug('Not found URL: %s',req.url);
    res.send({ error: 'Not found' });
    return;
});

app.use(function(err, req, res, next){
    res.status(err.status || 500);
    log.error('Internal error(%d): %s',res.statusCode,err.message);
    res.send({ error: err.message });
    return;
});

app.post('/registry/defineschema/:name', function(req, res) {
    var name = req.params.name;
    var fields = req.body.fields;
    registry.RegistryModel.findOne({name: name}, function (err, schema) {
        if(!!schema) {
            res.send({status: 'Registry with name "' + name + '"already exists', schema: schema})
        } else {
            var problems;
            if (fields && (problems = registry.findProblemsInFields(fields)).length == 0) {
                var registrySchema = new registry.RegistryModel(
                    {name: name,
                    fields: fields}
                );
                return registrySchema.save(function(err) {
                    if (!err) {
                        return res.send({status: 'OK', registrySchema: registrySchema})
                    } else {
                        log.error(err);
                        return res.send({ error: 'Server error' });
                    }
                });
            } else {
                return res.send({ error: 'Fields are defined not correctly: ' + problems + ' ' + fields});
            }
        }
        res.send({status: 'all is nice'});
    })
});

app.post('/:name/put', function(req, res) {
    var name = req.params.name;
    var body = req.body;
    registry.RegistryModel.findOne({name: name}, function (err, schema) {
        if (err) {
            log.error(err);
            return res.send({ error: 'Server error' });
        } else if(!schema) {
            var error = 'Registry with name "' + name + '" not found';
            log.error(error);
            res.statusCode = 404;
            return res.send(error);
        } else {
            log.info('in server method body = ' + body);
            registry.storeRecord(schema, body, function(err, result) {
                if (!err) {
                    res.send({status: 'OK', name: name, registryRecord: result})
                } else {
                    res.statusCode = 500;
                    return res.send({ error: err.message});
                }
            });
        }
    });
});

app.get('/registry/all', function(req, res) {
    return RegistryModel.find(function (err, registries) {
        if (!err) {
            return res.send(registries);
        } else {
            res.statusCode = 500;
            log.error('Internal error(%d): %s',res.statusCode,err.message);
            return res.send({ error: 'Server error' });
        }
    });
});

app.get('/:name/aggregate', function(req, res) {
    registry.RegistryModel.findOne({name: req.params.name}, function (err, schema) {
        if (err) {
            log.error(err);
            return res.send({ error: 'Server error' });
        } else if(!schema) {
            var error = 'Registry with name "' + req.params.name + '" not found';
            log.error(error);
            res.statusCode = 404;
            return res.send(error);
        } else {
            return registry.aggregate(schema, req.params, function(err, result) {
                return res.send(result);
            });
        }
    });
});

app.post('/registry/deleteschema/:name', function(req, res) {
    res.send('This is not implemented now');
});

app.get('/registry/get/:name', function(req, res) {
    res.send('This is not implemented now');
});

app.post('/registry/remove/:name');

app.get('/ErrorExample', function(req, res, next){
    next(new Error('Random error!'));
});

app.listen(config.get('port'), function(){
    log.info('Express server listening on port ' + config.get('port'));
});