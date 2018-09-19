'use strict';

const express = require('express')
var moment = require('moment-timezone')
var kafka = require('kafka-node');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var argv = require('optimist').argv;
var topic = argv.topic || 'test';

//var client = new Client('localhost:2181'),
var ConsumerGroup = kafka.ConsumerGroup,
mongojs = require('mongojs'),
db = mongojs('localhost:27017/events');

var topics = [topic];
var options = { host: '127.0.0.1:2181', protocol: 'roundrobin', sessionTimeout: 15000,  fromOffset: 'latest', groupId: 'consumer.js' };

var consumer = new ConsumerGroup(options, topics);

const app = express()



consumer.on('message', function (message) {
  var data= JSON.parse(message.value);

  if (data.event!= null){
    switch(data.event){
      case 1:
            db.collection('events').update(
          {
            question: data.question
          },
          {
            '$set': {
            test: data.testId,
            },
            '$inc': {
              event1: 1
            }
          },
          {
            upsert: true
          }, function(err, data) {
            if(err) {
              console.log('MONGO error updating document: ' + err);
            } else {
              console.log('MONGO updating document OK:' + message.value);
            }
          });
            break;

      case 2:
            db.collection('events').update(
          {
            question: data.question
          },
          {
            '$set': {
            test: data.testId,
            },
            '$inc': {
              event2: 1
            }
          },
          {
            upsert: true
          }, function(err, data) {
            if(err) {
              console.log('MONGO error updating document: ' + err);
            } else {
              console.log('MONGO updating document OK:' + message.value);
            }
          });
            break;

        case 3:
            db.collection('events').update(
          {
            question: data.question
          },
          {
            '$set': {
            test: data.testId,
            },
            '$inc': {
              event3: 1
            }
          },
          {
            upsert: true
          }, function(err, data) {
            if(err) {
              console.log('MONGO error updating document: ' + err);
            } else {
              console.log('MONGO updating document OK:' + message.value);
            }
          });
          break;  
    }
  }


  if (data.time!=null){
    db.collection('events').update(
          {
            testId: data.testId
          },
          {
            '$set': {
            time: data.time,
            }
          },
          {
            upsert: true
          }, function(err, data) {
            if(err) {
              console.log('MONGO error updating document: ' + err);
            } else {
              console.log('MONGO updating document OK:' + message.value);
            }
          });

  }

});

consumer.on('error', function (err) {
  console.log('error', err);
});

/*
* If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset
*/
consumer.on('offsetOutOfRange', function (topic) {
  topic.maxNum = 2;
  offset.fetch([topic], function (err, offsets) {
    if (err) {
      return console.error(err);
    }
    var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
    consumer.setOffset(topic.topic, topic.partition, min);
  });
});

process.on('SIGINT', function() {
  consumer.close(true, function() {
    process.exit();
  });
});

app.get('/',function(req,res){
    res.json({greeting:'Kafka Consumer is Listening'})
    consumer.on('message', function (message) {
      res.json(message.value);
    });
});

app.listen(5002,function(){
    console.log('Kafka consumer running at 5002')
});
