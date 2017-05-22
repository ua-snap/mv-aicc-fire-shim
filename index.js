var express = require('express');
var Promise = require('bluebird');
var parse = require('csv-parse');
var moment = require('moment');
var winston = require('winston');

var logger = new (winston.Logger)({
  transports: [
    new (winston.transports.Console)({
      timestamp: function() {
        return moment().format();
      },
      formatter: function(options) {
        // Return string will be passed to logger.
        return options.timestamp() +' '+ options.level.toUpperCase() +' '+ (options.message ? options.message : '') +
          (options.meta && Object.keys(options.meta).length ? '\n\t'+ JSON.stringify(options.meta) : '' );
      }
    })
  ]
});

var request = Promise.promisifyAll(require('request'), {multiArgs: true});
var _ = require('lodash');
const NodeCache = require('node-cache');
const cache = new NodeCache({ stdTTL: 3600, checkperiod: 3600 });

var app = express();

var activeFirePerimetersUrl = 'https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires_Perimeters/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=OBJECTID%2C+NAME%2C+ACRES%2C+PERIMETERDATE%2C+LATESTPERIMETER%2C+COMMENTS%2C+FIREID%2C+FIREYEAR%2C+UPDATETIME%2C+FPMERGEDDATE%2C+IRWINID&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=4326gdbVersion=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=&f=geojson';
var activeFiresUrl = 'https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires/MapServer/0/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=OBJECTID%2C+ID%2C+NAME%2C+LASTUPDATETIME%2C+LATITUDE%2C+LONGITUDE%2C+DISCOVERYDATETIME%2C+IADATETIME%2C+IASIZE%2C+CONTROLDATETIME%2C+OUTDATE%2C+ESTIMATEDTOTALACRES%2C+ACTUALTOTALACRES%2C+GENERALCAUSE%2C+SPECIFICCAUSE%2C+STRUCTURESTHREATENED%2C+STRUCTURESBURNED%2C+PRIMARYFUELTYPE%2C+FALSEALARM%2C+FORCESITRPT%2C+FORCESITRPTSTATUS%2C+RECORDNUMBER%2C+COMPLEX%2C+ISCOMPLEX%2C+IRWINID%2C+CONTAINMENTDATETIME%2C+CONFLICTIRWINID%2C+COMPLEXPARENTIRWINID%2C+MERGEDINTO%2C+MERGEDDATE%2C+ISVALID&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=4326&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&f=geojson';

var inactiveFirePerimetersUrl = 'https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires_Perimeters/FeatureServer/1/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=OBJECTID%2C+NAME%2C+ACRES%2C+PERIMETERDATE%2C+LATESTPERIMETER%2C+COMMENTS%2C+FIREID%2C+FIREYEAR%2C+UPDATETIME%2C+FPMERGEDDATE%2C+IRWINID&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=4326gdbVersion=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=&f=geojson';
var inactiveFiresUrl = 'https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires/MapServer/1/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=OBJECTID%2C+ID%2C+NAME%2C+LASTUPDATETIME%2C+LATITUDE%2C+LONGITUDE%2C+DISCOVERYDATETIME%2C+IADATETIME%2C+IASIZE%2C+CONTROLDATETIME%2C+OUTDATE%2C+ESTIMATEDTOTALACRES%2C+ACTUALTOTALACRES%2C+GENERALCAUSE%2C+SPECIFICCAUSE%2C+STRUCTURESTHREATENED%2C+STRUCTURESBURNED%2C+PRIMARYFUELTYPE%2C+FALSEALARM%2C+FORCESITRPT%2C+FORCESITRPTSTATUS%2C+RECORDNUMBER%2C+COMPLEX%2C+ISCOMPLEX%2C+IRWINID%2C+CONTAINMENTDATETIME%2C+CONFLICTIRWINID%2C+COMPLEXPARENTIRWINID%2C+MERGEDINTO%2C+MERGEDDATE%2C+ISVALID&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=4326&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&f=geojson';


var fireTimeSeriesUrl = 'https://fire.ak.blm.gov/content/aicc/Statistics%20Directory/Alaska%20Daily%20Stats%20-%202004%20to%20Present.csv';

// Used to set common headers for all responses
function setCommonHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', '*');
}

app.get('/', function (req, res) {
  getFireGeoJSON()
    // After fetching the merged data from cache or
    // an upstream fetch, it's available as fireGeoJSON
    // in the success handler below
    .then(function (fireGeoJSON) {
      setCommonHeaders(res);
      res.json({
        type: 'FeatureCollection',
        features: fireGeoJSON
      });
    })
    // Something failed upstream and the cache is stale,
    // return empty 500.
    .catch(function (err) {
      logger.error(err);
      res.status(500).send();
    });
});

app.get('/fire-time-series', function (req, res) {
  getFireTimeSeries()
    // After fetching the merged data from cache or
    // an upstream fetch, it's available as fireGeoJSON
    // in the success handler below
    .then(function (fireTimeSeries) {
      setCommonHeaders(res);
      res.json(fireTimeSeries);
    })
    // Something failed upstream and the cache is stale,
    // return empty 500.
    .catch(function (err) {
      logger.error(err);
      res.status(500).send();
    });
});

// Return current fire data; either fetch from cache, or
// update from upstream sources.
function getFireGeoJSON () {
  return new Promise(function (resolve, reject) {

    // Try cache...
    var fireGeoJSON = cache.get('fireGeoJSON');

    if (undefined === fireGeoJSON) {
      // Cache miss.
      logger.info('Attempting to update cache from upstream data...');

      // Grab both API requests asynchronously
      var urlList = [activeFirePerimetersUrl, activeFiresUrl, inactiveFirePerimetersUrl, inactiveFiresUrl];
      Promise.map(urlList, function (url) {
        return request.getAsync(url).spread(function (response, body) {
          if (response.statusCode === 200) {
            try {
              return [JSON.parse(body), url];
            } catch (err) {
              reject(new Error('Could not parse upstream JSON'));
            }
          } else {
            reject(new Error('Upstream service status code: ' + response.statusCode));
          }
        })
        .catch(function(err) {
          reject('Could not fetch or process upstream data.');
        });
      }).catch(function (err) {
        reject(err);
      }).then(function (results) {

          if(undefined !== results[0] && undefined !== results[1]) {
          logger.info('Upstream data fetched OK, processing and updating cache...');

          // Each element in the `results` is a two-element array,
          // first element is the data; 2nd is the URL.
          fireGeoJSON = processGeoJSON(results[0][0], results[1][0], results[2][0], results[3][0]);
          cache.set('fireGeoJSON', fireGeoJSON);
          resolve(fireGeoJSON);
        }
      });

    } else {
      // Cache hit, serve data immediately.
      resolve(fireGeoJSON);
    }
  });
};

function getFireTimeSeries () {
  return new Promise(function (resolve, reject) {
    // Try cache...
    var fireTimeSeries = cache.get('fireTimeSeries');

    if (undefined === fireTimeSeries) {
      // Cache miss.
      logger.info('Attempting to update fire timeseries cache from upstream CSV...');

      // These are all the years with 1+ million acres burned since 2004.
      var topYears = ['2004', '2015', '2005', '2009', '2010', '2013'];

      var startDay = 121; // May 1
      var endDay = 274;   // September 30

      // This endpoint will output the top years + current year.
      var currentYear = moment().format('YYYY');
      var outputYears = topYears.concat(currentYear);

      // Utility function to help build year/month/day tree, which is used later
      // on to fill in gaps and smooth out cumulative totals that go backwards.
      function setAcres(obj, year, month, day, acres) {
        if (year !== 'FireSeason') {
          if (obj[year] === undefined) {
            obj[year] = {};
          }

          if (obj[year][month] === undefined) {
            obj[year][month] = {};
          }

          if (obj[year][month][day] === undefined) {
            obj[year][month][day] = acres;
          }
        }
      }

      function parseData (data) {
        var parsedData = {};

        data.forEach(function (line) {
          var year = line[1];
          var month = line[2];
          var day = line[3];
          var acres = line[6];

          setAcres(parsedData, year, month, day, acres);
        });

        return parsedData;
      };

      function fixData (data) {
        var fixedData = {};

        for (var year in data) {
          if (data.hasOwnProperty(year)) {
            _.range(startDay, endDay).forEach(function (dayOfYear) {
              var month = moment().dayOfYear(dayOfYear).month() + 1;
              var day = moment().dayOfYear(dayOfYear).date();

              // Do not attempt to process future days.
              if (year !== currentYear || dayOfYear <= moment().dayOfYear()) {
                if (dayOfYear === startDay) {
                  // Set first day to zero if no value was provided in the CSV.
                  // Otherwise, use the value that was parsed from the CSV.
                  if (data[year][month] === undefined || data[year][month][day] == undefined) {
                    setAcres(fixedData, year, month, day, 0);
                  } else {
                    setAcres(fixedData, year, month, day, data[year][month][day]);
                  }
                } else {
                  // Yesterday is the day prior to the day currently being processed.
                  var yesterday = moment().dayOfYear(dayOfYear - 1).date();
                  var yesterdayMonth = moment().dayOfYear(dayOfYear - 1).month() + 1;

                  if(data[year][month] === undefined || data[year][month][day] == undefined) {
                    // If the current day has no value, use the value from the previous day.
                    setAcres(fixedData, year, month, day, fixedData[year][yesterdayMonth][yesterday]);
                  } else if (parseFloat(data[year][month][day]) < parseFloat(fixedData[year][yesterdayMonth][yesterday])) {
                    // If the day before has a value greater than the current day, use the value
                    // from the day before to enforce strict cumulative totals.
                    setAcres(fixedData, year, month, day, fixedData[year][yesterdayMonth][yesterday]);
                  } else {
                    // If there are no issues, simply use the value from the CSV.
                    setAcres(fixedData, year, month, day, data[year][month][day]);
                  }
                }
              }
            });
          }
        }

        return fixedData;
      };

      function formatData (data) {
        var formattedData = {};

        for (var year in data) {
          if (data.hasOwnProperty(year)) {
            if (_.includes(outputYears, year)) {
              formattedData[year] = {};
              formattedData[year].dates = [];
              formattedData[year].acres = [];

              for (var month in data[year]) {
                for (var day in data[year][month]) {
                  var dateLabel = moment.months(month - 1) + ' ' + day;
                  formattedData[year].dates.push(dateLabel);
                  formattedData[year].acres.push(data[year][month][day]);
                }
              }
            }
          }
        }

        return formattedData;
      };

      // Fetch the CSV file.
      request.getAsync(fireTimeSeriesUrl).spread(function (response, body) {
        // parsedData stores the data was it was found in the original CSV.
        var parsedData;

        if (response.statusCode === 200) {
          try {
            var parser = parse(body, {delimiter: ','}, function (err, data) {
              parsedData = parseData(data);
            });
          } catch (err) {
            reject(new Error('Could not parse upstream CSV'));
          }
        } else {
          reject(new Error('Upstream service status code: ' + response.statusCode));
        }

        parser.on('end', function () {
          // fixedData stores the data with data gaps filled and cumulative totals
          // strictly enforced by never decreasing throughout a year.
          var fixedData = fixData(parsedData);

          // fireTimeSeries stores only the years that will be output to the
          // endpoint, with the dates and acres stored in separate arrays to
          // make the data ready for use in Plotly.
          fireTimeSeries = formatData(fixedData);
          cache.set('fireTimeSeries', fireTimeSeries);
          resolve(fireTimeSeries);
        });
      }).catch(function(err) {
        reject('Could not fetch or process upstream data.');
      });
    } else {
      // Cache hit, serve data immediately.
      resolve(fireTimeSeries);
    }
  });
};

// Set up server variables and launch node HTTP server
var serverPort = process.env.OPENSHIFT_NODEJS_PORT || 3000;
var serverIpAddress = process.env.OPENSHIFT_NODEJS_IP || '127.0.0.1';

app.listen(serverPort, serverIpAddress, function () {
  logger.info('Server running on', serverIpAddress, ':', serverPort);
});

// Merge information from the two API endpoints into an array of GeoJSON Features.
function processGeoJSON (activeFirePerimeters, activeFires, inactiveFirePerimeters, inactiveFires) {

  // Function that formats the size of the fire into the desired format
  var parseAcres = function(a) {
    return parseFloat(a).toFixed(2);
  }

  // Function that formats the update time into the desired foramt
  var parseUpdatedTime = function(t) {
    return moment.utc(moment.unix(t / 1000)).format('MMMM D, h:mm a')
  }

  // Start by adding a few fields to each batch
  _.each(activeFirePerimeters.features, function (feature, index, list) {
    list[index].properties.active = true;
    list[index].properties.acres = parseAcres(feature.properties.ACRES);
    list[index].properties.updated = parseUpdatedTime(feature.properties.UPDATETIME);
  });
  _.each(inactiveFirePerimeters.features, function (feature, index, list) {
    list[index].properties.active = false;
    list[index].properties.acres = parseAcres(feature.properties.ACRES);
    list[index].properties.updated = parseUpdatedTime(feature.properties.UPDATETIME);
  });
  _.each(activeFires.features, function (feature, index, list) {
    list[index].properties.active = true;
    list[index].properties.acres = parseAcres(feature.properties.ESTIMATEDTOTALACRES);
    list[index].properties.updated = parseUpdatedTime(feature.properties.LASTUPDATETIME);
  });
  _.each(inactiveFires.features, function (feature, index, list) {
    list[index].properties.active = false;
    list[index].properties.acres = parseAcres(feature.properties.ESTIMATEDTOTALACRES);
    list[index].properties.updated = parseUpdatedTime(feature.properties.LASTUPDATETIME);
  });

  // Create a temporary data structure that is indexed in a useful way
  var indexedFirePerimeters = {};
  var allPerimeters = _.concat(activeFirePerimeters.features, inactiveFirePerimeters.features);
  _.each(allPerimeters, function (feature, index) {
    indexedFirePerimeters[feature.properties.IRWINID] = feature;
  });

  var indexedAllFires = {};
  var allFires = _.concat(activeFires.features, inactiveFires.features);
  _.each(allFires, function (feature, index) {
    indexedAllFires[feature.properties.IRWINID] = feature;
  });

  // Combine fire info with a perimeter, if available
  var mergedFeatures = [];
  _.each(indexedAllFires, function (feature, key) {
    var tempFeature = feature;

    if (undefined !== indexedFirePerimeters[key]) {
      // Grab polygon and add to other fire data
      tempFeature.geometry = indexedFirePerimeters[key].geometry;
    }
    mergedFeatures.push(tempFeature);
  });

  // Sometimes, there's a fire perimeter but no way to tie it to
  // the other list of info (!).
  _.each(indexedFirePerimeters, function(feature, key) {
    if(undefined === indexedAllFires[key]) {
      mergedFeatures.push(feature);
    }
  });

  return mergedFeatures;
}
