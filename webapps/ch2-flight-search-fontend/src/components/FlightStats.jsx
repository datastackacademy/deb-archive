import React, { Fragment, useState, useEffect } from 'react';
import PropTypes from 'prop-types';
import { Tabs, Tab, Paper } from '@material-ui/core';
import { fetchAirplaneInfo } from '../helpers/apiCalls';


const cancellationCode = (code) => {
  if (code === "A") { return "Carrier" }
  else if (code === "B") { return "Weather" }
  else if (code === "C") { return "National Air System" }
  else if (code === "D") { return "Security" }
  else { return "None" }
}

const CanceledStats = ({ cancelCode }) => {
  return (
    <Fragment>
      <Paper variant="outlined">
        <table>
          <tr>
            <td><h3>Cancelled</h3></td>
            <td>{`Cancellation Code: ${cancelCode}`}</td>
            <td>{`Cancellation Reason: ${cancellationCode(cancelCode)}`}</td>
          </tr>
        </table>
      </Paper>
    </Fragment>);
};

const TabPanel = ({ children, value, index, ...other }) => {
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`flight-stats-panel-${index}`}
      aria-labelledby={`flight-stats-panel-${index}`}
      {...other}
    >
      {value === index && (
        <Paper variant="outlined">
          {children}
        </Paper>
      )}
    </div>
  );
}

TabPanel.propTypes = {
  children: PropTypes.node,
  index: PropTypes.any.isRequired,
  value: PropTypes.any.isRequired,
};

const a11yProps = (index) => {
  return {
    id: `flight-stats-${index}`,
    'aria-controls': `flightstats-${index}`,
  };
}


const FlightStats = ({ flight: { day_of_week, flight_date, airline, tailnumber, flight_number, src, src_city, src_state, dest, dest_city, dest_state, departure_time, actual_departure_time, departure_delay, taxi_out, wheels_off, wheels_on, taxi_in, arrival_time, actual_arrival_time, arrival_delay, cancelled, cancellation_code, flight_time, actual_flight_time, air_time, flights, distance, airline_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay, flightDate_airline_flightNumber } }) => {
  const [value, setValue] = useState(0);
  const [airplaneData, setAirplaneData] = useState();

  const handleChange = (event, newValue) => {
    setValue(newValue);
  };

  const Loading = () => {
    return (
      <div className="loading">
        <h1>Loading<span className="animate-flicker">...</span></h1>
      </div>
    )
  };

  useEffect(() => {
    const tailnum = tailnumber[0] === 'N' ? tailnumber.slice(1) : tailnumber;
    fetchAirplaneInfo('http://localhost:5000/', tailnum)
      .then(res => res.json())
      .then(res => {
        setAirplaneData(res);
        console.log(res);
      });

  }, []);



  return (
    <Fragment>
      {airplaneData ?
        (<Fragment>
          <Tabs value={value} onChange={handleChange} aria-label="flight-stats-tabs" indicatorColor="primary">
            <Tab label="Flight Stats" className="flightModalTab" {...a11yProps(0)} />
            <Tab label="Departure Stats" className="flightModalTab" {...a11yProps(1)} />
            <Tab label="Arrival Stats" className="flightModalTab" {...a11yProps(2)} />
            <Tab label="Delay Stats" className="flightModalTab" {...a11yProps(3)} />
          </Tabs>
          <TabPanel value={value} index={0}>
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
          </TabPanel>
          <TabPanel value={value} index={1}>
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
          </TabPanel>
          <TabPanel value={value} index={2}>
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
          </TabPanel>
          <TabPanel value={value} index={3}>
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
          </TabPanel>
        </Fragment>
        ) :
        <Loading />}
    </Fragment>);
}

export { CanceledStats, FlightStats };
