/**
 * Created by mgtriffid on 09.04.2014.
 */
var mongoose    = require('mongoose');
var log         = require('./log')(module);
var config      = require('./config');

var listOfTypes = ["date", "string", "number"];

mongoose.connect(config.get('mongoose:uri2'));
var db = mongoose.connection;

db.on('error', function (err) {
    log.error('connection error:', err.message);
});
db.once('open', function callback () {
    log.info("Connected to DB!");
});

var Schema = mongoose.Schema;

// Schemas
var FieldSchema = new Schema({
    name: {type: String},
    type: {type: String},
    isKey: {type: Boolean},
    isIndexed: {type: Boolean}
});

var RegistrySchema = new Schema({
    name: { type: String, required: true },
    fields: [FieldSchema]
});

var getModelsByName= function(schema, callback) {
    var actualModel;
    var archiveModel;
    if (!!mongoose.modelSchemas[schema.name]) {
        actualModel = mongoose.model(schema.name);
        archiveModel = mongoose.model('Archive_' + schema.name);
    } else {
        //Я называю это "Ебанько-код"
        var mySchema = new mongoose.Schema;
        schema.fields.forEach(function (f) {
            var arg = {};
            arg[f.name] = {type: String};
            mySchema.add(arg);
        });
        var keyFields = schema.fields.filter(function(f){return f.isKey})
        var indexing = {};

        keyFields.forEach(function (f) {
            indexing[f.name] = 1;
        });
        mySchema.add({'modificationDate': {type: Date}});
        mySchema.add({'modifiedBy': {type: String}});
        mySchema.add({'modificationComment': {type: String}});

        mySchema.index(indexing);
        archiveModel = mongoose.model('Archive_' + schema.name, mySchema);
        mySchema.index(indexing);
        actualModel = mongoose.model(schema.name, mySchema);
    }
    return callback(null, actualModel, archiveModel);
};

//TODO: Сообщать о всех ошибках, а не только о первой попавшейся
this.findProblemsInFields = function(fields) {
    var problems = '';
    log.info();
    try {
        log.info(fields);
        fields.forEach(function(field){
            if (field.name == undefined) {
                problems += 'Name not found for field "' + field.toString() + '"; ';
                return;
            }
            if (field.type == undefined) {
                problems += 'Type not found for field "' + field.toString() + '"; ';
                return;
            }
            log.info(field);
            log.info(typeof(field));
            if (listOfTypes.indexOf(field.type) == -1) {
                log.error('Unexpected type of variable "' + field.name + '", one of following expected: ' + listOfTypes);
                problems += 'Unexpected type of variable "' + field.name + '"';
            }
        });
        log.info('Iteration complete');
    }
    catch (e) {
        log.error('Failed to parse ' + e);
        return 'Failed to validate fields';
    }
    return problems;
};

this.storeRecord = function(registrySchema, body, callback){
    var fieldNames = registrySchema.fields.map(function(f) {
        return f.name;
    });
    for (parameter in body.fields) {
        if (fieldNames.indexOf(parameter) == -1) {
            return callback(new Error('Schema doesn\'t allow field "' + parameter + '"'), 'FAIL');
        }
    }

    if (!body.modifiedBy) {
        return callback(new Error('Modified by not found in request'), 'FAIL');
    }

    var keyFields = registrySchema.fields.filter(function(f){return f.isKey})
    var MyModel;
    var MyArchiveModel;

    getModelsByName(registrySchema, function(err, actualModel, archiveModel) {
        MyModel = actualModel;
        MyArchiveModel = archiveModel;
    });
    var values = {};
    var uniquenessCheck = {}
    keyFields.forEach(function(f){
        if(!body.fields[f.name]) {
            var s = 'Key value "' + f.name + '" not found in request';
            return callback(new Error(s), s);
        }
        uniquenessCheck[f.name] = body.fields[f.name];
    });
    for (parameter in body.fields) {
        values[parameter] = body.fields[parameter] || '';
    }
    values.modificationDate = new Date();
    values.modifiedBy = body.modifiedBy || '';
    values.modificationComment = body.modificationComment || '';
    var myInstance = new MyModel(values);
//    myInstance.save(callback);
    MyModel.findOne(uniquenessCheck, function(err, record) {
        log.info('In callback after u-check')
        if (err) {
            return callback(err, 'Failed to perform uniquness check');
        } else if (!!record) {
            log.info('Record found');
            record._id = null;
            var myArchiveInstance = new MyArchiveModel(record);
            myArchiveInstance.save(function(err, doc) {
                log.info('In callback after save archive instance');
                return MyModel.findOneAndUpdate(uniquenessCheck, values, callback);});
        } else {
            return myInstance.save(callback(null, 'Record created successfully'));
        }
    });
};
//TODO: обработать параметры
this.aggregate = function(registrySchema, params, callback) {
    var fieldNames = registrySchema.fields.map(function(f) {
        return f.name;
    });

    var keyFields = registrySchema.fields.filter(function(f){return f.isKey})
    var MyModel;
    var MyArchiveModel;

    getModelsByName(registrySchema, function(err, actualModel, archiveModel) {
        MyModel = actualModel;
        MyArchiveModel = archiveModel;
    });
    var conditions ={};
    MyModel.find(conditions, function(err, result) {
        var keyConditions = {};
        var counter = result.length;
        log.info('counter = ' + counter);

        var aggregateHistory = function(items) {
            items.sort(function(a, b){
                return new Date(b.modificationDate) - new Date(a.modificationDate);
            });
            var history = '';
            var curr;
            var prev;
            for (var i = items.length - 1; i>0; i--) {
                var difference = '';
                prev = items[i];
                curr = items[i-1];
                registrySchema.fields.forEach(function(f) {
                    log.info(f.name);
                    if (curr[f.name] != prev[f.name]) {
                        difference += 'Field "' + f.name + '" changed from "' + prev[f.name] + '" to "' + curr[f.name] + '";\n';
                    }
                });
                if (difference) {
                    history += curr.modifiedBy + ' changed the record on ' + curr.modificationDate + ': ' + difference;
                }
            }
            return history;
        };

        var aggregateRecord = function(k) {
            if (k) {
                var rs = result[k - 1];
                keyConditions = {};
                keyFields.forEach(function(f) {
                    keyConditions[f.name] = rs[f.name];
                });
                console.log(k);

                MyArchiveModel.find(keyConditions, function(error, archiveRecords) {
                    archiveRecords.push(rs);
                    result[k-1]['_doc']['history'] = aggregateHistory(archiveRecords);
                    aggregateRecord(k - 1);
                });
            } else {
                return callback(err, result);
            }
        };
        aggregateRecord(counter)
    })
};

var RegistryModel = mongoose.model('Registry', RegistrySchema);

module.exports.RegistryModel = RegistryModel;