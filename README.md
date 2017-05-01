# Utility to create merged GeoJSON from AICC fire data

This is a simple node service which fetches data from the AICC web services and creates a merged GeoJSON containing consistent fields for both fires with/without perimeters.

## Installation

Requires `node 4.4.2`, `npm 2.15.x`.

```
npm install
npm test // runs eslint
npm start
```

After running locally, the node service will be available at `localhost:3000`.