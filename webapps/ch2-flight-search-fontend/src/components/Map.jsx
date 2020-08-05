import React, {Fragment, useState} from 'react';
import GoogleMapReact from 'google-map-react';

const AirportPin = ({airport, source, lat, lng}) => {

    const sourcePin = (
        <div className="pin pin-src">
            <div className="pin-content">
                <h2>{airport.name}</h2>
            </div>
        </div>
    );

    const destinationPin = (
        <div className="pin pin-dest">
            <div className="pin-content">
                <h2>{airport.name}</h2>
            </div>
        </div>
    );

    return (
        <Fragment>
            {source? sourcePin : destinationPin}
        </Fragment>
    )
}


const Key = () => {
    return (
        <div className = "key">
            <div className="pin-src key-content">Source Airport</div>
            <div className="pin-dest key-content">Destination Airport</div>
        </div>
    )
}
const Map = ({airportInfo}) => {
    const flightPath = [{lat:airportInfo.src.lat , lng: airportInfo.src.long}, {lat:airportInfo.dest.lat , lng:airportInfo.dest.long }]
    const avgLat = (airportInfo.src.lat + airportInfo.dest.lat)/2;
    const avgLng = (airportInfo.src.long + airportInfo.dest.long)/2;
    const center = {lat: avgLat, lng: avgLng};
    const defaultMapInfo = {center: center, zoom: 4}
    const [keyLocation, setKeyLocation] = useState(null);
    
    const drawPath = (map, maps, flightPath) => {
        var lineSymbol = {
            path: 'M 0,-1 0,1',
            strokeOpacity: 1,
            strokeColor: '#183B56',
            scale: 4
          };
        const path = new maps.Polyline({
            path: flightPath,
            // geodesic: true,
            // strokeColor: '#183B56',
            strokeOpacity: 0,
            icons:[{
                icon:lineSymbol,
                offset: '0',
                repeat: '20px'
            }]
            // strokeWeight: 2
        });
        path.setMap(map);
    }

    return (
        <div style={{ height: "calc(66.67vh - 10rem)", width: "100%" }}>
            <GoogleMapReact
                
                bootstrapURLKeys={{key: "AIzaSyBtP_A6dX5ZPe6TzH9U7A-tbGew4vCh7KU"}}
                defaultCenter={defaultMapInfo.center}
                defaultZoom={defaultMapInfo.zoom}
                options={{clickableIcons: false}}
                onGoogleApiLoaded={({map, maps}) => {
                    drawPath(map,maps, flightPath);
                    const SWCorner = map.getBounds().getSouthWest();
                    setKeyLocation({lat: SWCorner.lat(), lng: SWCorner.lng()});
                }}
                onChange={(map) => {
                    setKeyLocation(map.bounds.sw);
                }}
                yesIWantToUseGoogleMapApiInternals
            >
                {keyLocation && <Key lat={keyLocation.lat} lng={keyLocation.lng}/>}
                {airportInfo && <AirportPin source={true} lat={airportInfo.src.lat} lng={airportInfo.src.long} airport={airportInfo.src}/>}
                {airportInfo && <AirportPin source={false} lat={airportInfo.dest.lat} lng={airportInfo.dest.long} airport={airportInfo.dest}/>}
            </GoogleMapReact>
        </div>
    )
}

export default Map;