import React from 'react';
import { fetchFlights } from '../helpers/apiCalls';
import FlightRow from './FlightRow'
import {ExpansionPanel, ExpansionPanelSummary, ExpansionPanelDetails} from "@material-ui/core";

const AirlineContainer = ({rank, sectionName, expanded, airlines, airlineGroup,clickedAirline, setClickedAirline, setCurrentFlight, setDisplayModal}) => {

  const handleClick = () => {
    if(sectionName === clickedAirline){
      setClickedAirline(null);
    }else{
      setClickedAirline(sectionName);
    }
  }

  return (
    <ExpansionPanel expanded={expanded} id={sectionName} className="airline-container" onClick={handleClick}>

      <ExpansionPanelSummary>{`${rank}. ${airlines[airlineGroup.airline]}`}</ExpansionPanelSummary>
      <ExpansionPanelDetails>
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Flight Number</th>
              <th>Departure Delay</th>
              <th>Arrival Delay</th>
            </tr>
          </thead>
          <tbody>
            {airlineGroup.flights.map((flight, i) => <FlightRow key={`flightRow${i}`} flight={flight} setCurrentFlight={setCurrentFlight} setDisplayModal={setDisplayModal} />)}
          </tbody>
        </table>
      </ExpansionPanelDetails>
    </ExpansionPanel>
    );
}

export default AirlineContainer;