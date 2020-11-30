import React, { useState } from 'react';
import { Button} from '@material-ui/core';
import { formatDate } from '../helpers/checker';
import { CanceledStats, FlightStats } from './FlightStats';
import FlightModalGraphs from './FlightModalGraphs';

const FlightModal = ({ airlines, allFlights, filter, filteredFlights, setDisplayModal, flight }) => {
  const findingCurrentAirlineGroup = (currentAirline, airlineGroups) => {
    for (let i = 0; i < airlineGroups.length; i++) {
      if (airlineGroups[i].airline === currentAirline) {
        return airlineGroups[i];
      }
    }
  }
  // eslint-disable-next-line
  const [currentAirlineGroup, setCurrentAirlineGroup] = useState(findingCurrentAirlineGroup(flight.airline, filteredFlights));

  return (
    <div className="modal">
      <Button color="primary" aria-label="close button" className="close" onClick={() => setDisplayModal(false)}>X</Button>
      <h1>{`Flight Number ${flight.flight_number}, ${airlines[flight.airline]}`}</h1>
      <h2>{`${formatDate(flight.flight_date, "dddd, LL")}`}</h2>
      {/* <h2>{`${src} (${src_city},${src_state}) to ${dest} (${dest_city},${dest_state})`}</h2> */}
      <h2>{`${flight.src} (${flight.src_city}, ${flight.src_state}) to ${flight.dest} (${flight.dest_city})`}</h2>
      <h3>{`Tail Number: ${flight.tailnumber}`}</h3>
      <FlightModalGraphs flight={flight} currentAirlineGroup={currentAirlineGroup} allFlights={allFlights} airlines={airlines} />
      <h2>Flight Data</h2>
      <div className="modal-stats">
        {flight.cancelled ? <CanceledStats cancelCode={flight.cancellation_code} /> : <FlightStats flight={flight} />}
      </div>
    </div>
  );
}

export default FlightModal;