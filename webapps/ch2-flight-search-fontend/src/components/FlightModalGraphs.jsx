import React, { useState, useEffect } from 'react';
import PropTypes from 'prop-types';
import { Tabs, Tab, Paper } from '@material-ui/core';
import Chart from 'chart.js';
import { colorArray } from '../helpers/checker';

const TabPanel = ({ children, value, index, ...other }) => {
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`flight-graphs-${index}`}
      aria-labelledby={`flight-graph-tab-${index}`}
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
    id: `flight-graph-tab-${index}`,
    'aria-controls': `flight-graphs-${index}`,
  };
}


const FlightModalGraphs = ({flight, currentAirlineGroup, allFlights, airlines}) => {
  const [value, setValue] = useState(0);

  const handleChange = (event, newValue) => {
    setValue(newValue);
  };

  useEffect(() => {
    if (value === 0) {
      const ctx = document.getElementById("departure-graph");

      let initialDeparture = 0;
      const allFlightDepartDelayAvg = allFlights.reduce((accumulator, flight) => {
        return accumulator + flight.departure_delay;
      }, initialDeparture) / allFlights.length;
      const departData = [flight.arrival_delay, currentAirlineGroup["depart_delay_avg"], allFlightDepartDelayAvg]

      new Chart(ctx, {
        type: 'bar',
        data: {
          labels: [flight.tailnumber, airlines[flight.airline], "All Flights"],
          datasets: [{
            label: 'Departure Delay (minutes)', //make this dynamic
            data: departData,
            backgroundColor: colorArray(departData, "y"),
            borderWidth: 1
          }]
        },
        options: {
          maintainAspectRatio: false,
          scales: {
            yAxes: [{
              ticks: {
                beginAtZero: true
              }
            }]
          }
        }
      });
    } else {
      let initialArrival = 0;
      const allFlightArrivalDelayAvg = allFlights.reduce((accumulator, flight) => {
        return accumulator + flight.arrival_delay;
      }, initialArrival) / allFlights.length;
      const arrivalData = [flight.arrival_delay, currentAirlineGroup["arrival_delay_avg"], allFlightArrivalDelayAvg];

      const ctx2 = document.getElementById("arrival-graph");
      new Chart(ctx2, {
        type: 'bar',
        data: {
          labels: [flight.tailnumber, airlines[flight.airline], "All Flights"],
          datasets: [{
            label: 'Arrival Delay (minutes)',
            data: arrivalData,
            backgroundColor: colorArray(arrivalData, "g"),
            borderWidth: 1
          }]
        },
        options: {
          maintainAspectRatio: false,
          scales: {
            yAxes: [{
              ticks: {
                beginAtZero: true
              }
            }]
          }
        }
      });
    }
  });

  return (
    <React.Fragment>
      <Tabs value={value} onChange={handleChange} aria-label="flight-modal-graph-tabs" indicatorColor="primary">
        <Tab label="Departure Delay" className="flightModalTab" {...a11yProps(0)} />
        <Tab label="Arrival Delay" className="flightModalTab" {...a11yProps(1)} />
      </Tabs>
      <TabPanel value={value} index={0}>
        <canvas id="departure-graph" width="400" height="400"></canvas>
      </TabPanel>
      <TabPanel value={value} index={1}>
        <canvas id="arrival-graph" width="400" height="400"></canvas>
      </TabPanel>
    </React.Fragment>
  )
};

export default FlightModalGraphs;
