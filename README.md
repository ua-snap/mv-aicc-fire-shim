# Utility to create merged GeoJSON from AICC fire data

This is a simple node service which fetches data from the AICC web services and creates a merged GeoJSON containing consistent fields for both fires with/without perimeters.

## Installation

```
npm install
npm test // runs eslint
npm start
```

After running locally, the node service will be available at `localhost:3000`.

If there are certificate validation errors, it's necessary to bypass that by doing this:

```
export NODE_DEBUG=true
```

Be sure and set this env var in the AWS environment if needed for production.