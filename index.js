var express = require('express');
var Promise = require('bluebird');
var request = Promise.promisifyAll(require('request'), {multiArgs: true});
var _ = require('lodash');
const NodeCache = require('node-cache');
const cache = new NodeCache({ stdTTL: 3600, checkperiod: 3600 });

var app = express();

var firePerimetersUrl = 'https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires_Perimeters/FeatureServer/1/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=OBJECTID%2C+NAME%2C+ACRES%2C+PERIMETERDATE%2C+LATESTPERIMETER%2C+COMMENTS%2C+FIREID%2C+FIREYEAR%2C+UPDATETIME%2C+FPMERGEDDATE%2C+IRWINID&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=4326gdbVersion=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=&f=geojson';
var allFiresUrl = 'https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Fires/MapServer/1/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=OBJECTID%2C+NAME%2C+LASTUPDATETIME%2C+LATITUDE%2C+LONGITUDE%2C+DISCOVERYDATETIME%2C+IADATETIME%2C+IASIZE%2C+CONTROLDATETIME%2C+OUTDATE%2C+ESTIMATEDTOTALACRES%2C+ACTUALTOTALACRES%2C+GENERALCAUSE%2C+SPECIFICCAUSE%2C+STRUCTURESTHREATENED%2C+STRUCTURESBURNED%2C+PRIMARYFUELTYPE%2C+FALSEALARM%2C+FORCESITRPT%2C+FORCESITRPTSTATUS%2C+RECORDNUMBER%2C+COMPLEX%2C+ISCOMPLEX%2C+IRWINID%2C+CONTAINMENTDATETIME%2C+CONFLICTIRWINID%2C+COMPLEXPARENTIRWINID%2C+MERGEDINTO%2C+MERGEDDATE%2C+ISVALID&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=4326&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&f=geojson';

app.get('/', function (req, res) {
  getFireGeoJSON()
    // After fetching the merged data from cache or
    // an upstream fetch, it's available as fireGeoJSON
    // in the success handler below
    .then(function (fireGeoJSON) {
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', '*');
      res.json({
        type: 'FeatureCollection',
        features: fireGeoJSON
      });
    })
    // Something failed upstream and the cache is stale,
    // return empty 500.
    .catch(function (err) {
      console.log(err);
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
      console.info('Attempting to update cache from upstream data...');

      // Grab both API requests asynchronously
      var urlList = [firePerimetersUrl, allFiresUrl];
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
          console.log('Upstream data fetched OK, processing and updating cache...');

          // Each element in the `results` is a two-element array,
          // first element is the data; 2nd is the URL.
          fireGeoJSON = processGeoJSON(results[0][0], results[1][0]);
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

// Set up server variables and launch node HTTP server
var serverPort = process.env.OPENSHIFT_NODEJS_PORT || 3000;
var serverIpAddress = process.env.OPENSHIFT_NODEJS_IP || '127.0.0.1';

app.listen(serverPort, serverIpAddress, function () {
  console.log('Server running on', serverIpAddress, ':', serverPort);
});

// Merge information from the two API endpoints into an array of GeoJSON Features.
function processGeoJSON (firePerimeters, allFires) {
    // Create a temporary data structure that is indexed in a useful way
  var indexedFirePerimeters = {};
  _.each(firePerimeters.features, function (feature, index) {
    indexedFirePerimeters[feature.properties.IRWINID] = feature;
  });

  var indexedAllFires = {};
  _.each(allFires.features, function (feature, index) {
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
  return mergedFeatures;
}
