//what is airline delay?
import React, {Fragment, useEffect, useState} from 'react';
import {Button, ExpansionPanel, ExpansionPanelSummary, ExpansionPanelDetails, Tabs, Tab, AppBar} from '@material-ui/core';
import {formatDate} from '../helpers/checker'
import Chart from "chart.js";
import {colorArray} from '../helpers/checker';

const FlightModal = ({airlines, allFlights, filter, filteredFlights, setDisplayModal, flight:{day_of_week, flight_date, airline, tailnumber, flight_number, src, src_city, src_state, dest, dest_city, dest_state, departure_time, actual_departure_time, departure_delay, taxi_out, wheels_off, wheels_on, taxi_in, arrival_time, actual_arrival_time, arrival_delay, cancelled, cancellation_code, flight_time, actual_flight_time, air_time, flights, distance, airline_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay, flightDate_airline_flightNumber}}) => {
    const [graphDepart, setGraphDepart] = useState(null);
    const [graphArrival, setGraphArrival] = useState(null);
    const [graphValue, setGraphValue] = useState(0);

    const findingCurrentAirlineGroup = (currentAirline, airlineGroups) => {
        for(let i=0; i < airlineGroups.length; i++){
            if(airlineGroups[i].airline == currentAirline){
                return airlineGroups[i];
            }
        }
    }
    const [currentAirlineGroup, setCurrentAirlineGroup] = useState(findingCurrentAirlineGroup(airline, filteredFlights));


    useEffect(() => {
        const canvasD = document.getElementById('flight-depart');
        const ctxD = canvasD.getContext("2d");

        const canvasA = document.getElementById('flight-arrival');
        const ctxA = canvasA.getContext("2d");


        
        if(graphDepart){
          graphDepart.destroy();
          setGraphDepart();
        }
        if(graphArrival){
            graphArrival.destroy();
            setGraphArrival();
          }
        

        let initialDeparture = 0;
        const allFlightDepartDelayAvg = allFlights.reduce((accumulator, flight) => {
            return accumulator + flight.departure_delay;
        }, initialDeparture)/allFlights.length;
        const departData = [arrival_delay, currentAirlineGroup["depart_delay_avg"],  allFlightDepartDelayAvg]

        var departChart = new Chart(ctxD, {
          type: 'bar',
          data: {
              labels: [tailnumber, airlines[airline], "All Flights"],
              datasets: [{
                  label: 'Departure Delay (minutes)', //make this dynamic
                  data: departData,
    

                  backgroundColor: colorArray(departData, "y"),
                  borderWidth: 1
              }]
          },
          options: {
              scales: {
                  yAxes: [{
                      ticks: {
                          beginAtZero: true
                      }
                  }]
              }
          }
        });
        setGraphDepart(departChart);

        let initialArrival = 0;
        const allFlightArrivalDelayAvg = allFlights.reduce((accumulator, flight) => {
            return accumulator + flight.arrival_delay;
        }, initialArrival)/allFlights.length;
        const arrivalData = [arrival_delay, currentAirlineGroup["arrival_delay_avg"],  allFlightArrivalDelayAvg];

        var arrivalChart = new Chart(ctxA, {
            type: 'bar',
            data: {
                labels: [tailnumber, airlines[airline], "All Flights"],
                datasets: [{
                    label: 'Arrival Delay (minutes)',
                    data: arrivalData,
      
                    backgroundColor: colorArray(arrivalData, "g"),

                    borderWidth: 1
                }]
            },
            options: {
                scales: {
                    yAxes: [{
                        ticks: {
                            beginAtZero: true
                        }
                    }]
                }
            }
          });
          setGraphArrival(arrivalChart);
      }, [tailnumber]);

    const cancellationCode = (code) => {
        if("A"){return "Carrier"}
        else if("B"){return "Weather"}
        else if("C"){return "National Air System"}
        else if ("D"){return "Secruity"}
        else{return "None"}
    }
    const canceled_stats = (
        <Fragment>
            <ExpansionPanel>
                <ExpansionPanelSummary>Arrival Stats</ExpansionPanelSummary>
                <ExpansionPanelDetails>
                    <h3>Canceled</h3>
                    <p>{`Cancellation Code: ${cancellation_code}`}</p>
                    <p>{`Cancellation Reason: ${cancellationCode(cancellation_code)}`}</p>
                </ExpansionPanelDetails>
            </ExpansionPanel>
        </Fragment>
    );

    const flight_stats = (
        <Fragment>
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
                            <td>{airline_delay?airline_delay:0}</td>
                            <td>{weather_delay}</td>
                            <td>{nas_delay}</td>
                            <td>{security_delay}</td>
                            <td>{late_aircraft_delay}</td>
                        </tr>
                    </tbody>
                </table>
                </ExpansionPanelDetails>
            </ExpansionPanel>
        </Fragment>
    );

    const handleGraphTabChange = (event, newValue) => {
        setGraphValue(newValue);
    };

    return (
        <div className="modal">
            <Button color="primary" aria-label="close button" className="close" onClick={()=>setDisplayModal(false)}>X</Button>
                <h1>{`Flight Number ${flight_number}, ${airlines[airline]}`}</h1>
                <h2>{`${formatDate(flight_date, "dddd, LL")}`}</h2>
                {/* <h2>{`${src} (${src_city},${src_state}) to ${dest} (${dest_city},${dest_state})`}</h2> */}
                <h2>{`${src} (${src_city}, ${src_state}) to ${dest} (${dest_city})`}</h2>
                <h3>{`Tail Number: ${tailnumber}`}</h3>

                <Tabs value={graphValue}  onChange={handleGraphTabChange} variant="standard" aria-label="Arrival and Departure comparison graphs">
                    <Tab label="Arrival" />
                    <Tab label="Departure" />
                </Tabs>

                <div className="canvas-container">
                    
                    <canvas id="flight-depart" height="50px" width="50px"></canvas>
                    <canvas id="flight-arrival" height="50px" width="50px"></canvas>
                </div>
                <div className="modal-stats">
                {cancelled ? canceled_stats : flight_stats}
                </div>
                
        </div>
    );
}

export default FlightModal;