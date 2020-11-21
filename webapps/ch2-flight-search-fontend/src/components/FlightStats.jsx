import React from 'react'
import {ExpansionPanel, ExpansionPanelSummary, ExpansionPanelDetails} from '@material-ui/core';


const cancellationCode = (code) => {
  if(code === "A"){return "Carrier"}
  else if(code === "B"){return "Weather"}
  else if(code === "C"){return "National Air System"}
  else if (code === "D"){return "Security"}
  else{return "None"}
}

const CanceledStats = ({cancellationCode}) => {
  return (
  <React.Fragment>
      <ExpansionPanel>
          <ExpansionPanelSummary>Arrival Stats</ExpansionPanelSummary>
          <ExpansionPanelDetails>
              <table>
                <tr>
                  <td><h3>Cancelled</h3></td>
                  <td>{`Cancellation Code: ${cancellationCode}`}</td>
                  <td>{`Cancellation Reason: ${cancellationCode(cancellationCode)}`}</td>
                </tr>
              </table>
          </ExpansionPanelDetails>
      </ExpansionPanel>
  </React.Fragment>)
};



const FlightStats = ({flight:{day_of_week, flight_date, airline, tailnumber, flight_number, src, src_city, src_state, dest, dest_city, dest_state, departure_time, actual_departure_time, departure_delay, taxi_out, wheels_off, wheels_on, taxi_in, arrival_time, actual_arrival_time, arrival_delay, cancelled, cancellation_code, flight_time, actual_flight_time, air_time, flights, distance, airline_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay, flightDate_airline_flightNumber}}) => {
  return (
    <React.Fragment>
      <ExpansionPanel>
        <ExpansionPanelSummary>Flight Stats</ExpansionPanelSummary>
        <ExpansionPanelDetails>
          <table>
            <thead>
              <tr>
                <th>Flight Legs</th>
                <th>Distance</th>
                <th>Flight Time</th>
                <th>Actual Flight Time</th>
                <th>Air Time</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>{flights}</td>
                <td>{distance}</td>
                <td>{flight_time}</td>
                <td>{actual_flight_time}</td>
                <td>{air_time}</td>
              </tr>
            </tbody>
          </table>
          <table>
            <thead>
              <tr>
                <th>Wheels On</th>
                <th>Wheels Off</th>
                <th>Taxi In</th>
                <th>Taxi Out</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>{wheels_off}</td>
                <td>{wheels_on}</td>
                <td>{taxi_in}</td>
                <td>{taxi_out}</td>
              </tr>
            </tbody>
          </table>
        </ExpansionPanelDetails>
      </ExpansionPanel>

      <ExpansionPanel>
        <ExpansionPanelSummary>Departure Stats</ExpansionPanelSummary>
        <ExpansionPanelDetails>
          <table>
            <thead>
              <tr>
                <th>Scheduled Departure</th>
                <th>Actual Departure</th>
                <th>Departure Delay (min)</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>{departure_time}</td>
                <td>{actual_departure_time}</td>
                <td>{departure_delay}</td>
              </tr>
            </tbody>
          </table>
        </ExpansionPanelDetails>
      </ExpansionPanel>

      <ExpansionPanel>
        <ExpansionPanelSummary>Arrival Stats</ExpansionPanelSummary>
        <ExpansionPanelDetails>
          <table>
            <thead>
              <tr>
                <th>Scheduled Arrival</th>
                <th>Actual Arrival</th>
                <th>Arrival Delay (min)</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>{arrival_time}</td>
                <td>{actual_arrival_time}</td>
                <td>{arrival_delay}</td>
              </tr>
            </tbody>
          </table>
        </ExpansionPanelDetails>
      </ExpansionPanel>

      <ExpansionPanel>
        <ExpansionPanelSummary>Delay Stats</ExpansionPanelSummary>
        <ExpansionPanelDetails>
          <table>
            <thead>
              <tr>
                <th>Airline Delay</th>
                <th>Weather Delay</th>
                <th>National Air System Delay</th>
                <th>Secruity Delay</th>
                <th>Late Aircraft Delay</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                {/* if airline delay is ever changed to 0, can change this back */}
                <td>{airline_delay ? airline_delay : 0}</td>
                <td>{weather_delay}</td>
                <td>{nas_delay}</td>
                <td>{security_delay}</td>
                <td>{late_aircraft_delay}</td>
              </tr>
            </tbody>
          </table>
        </ExpansionPanelDetails>
      </ExpansionPanel>
    </React.Fragment>
  )
}

export {CanceledStats, FlightStats};
