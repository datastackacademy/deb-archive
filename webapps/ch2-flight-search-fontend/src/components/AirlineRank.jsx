import React, {useEffect, useState} from 'react';

// Material-UI
import InputLabel from '@material-ui/core/InputLabel';
import MenuItem from '@material-ui/core/MenuItem';
import FormControl from '@material-ui/core/FormControl';
import Select from '@material-ui/core/Select';

//Chart.js
import Chart from 'chart.js';

//helper functions
import {filterFlights} from '../helpers/filterRank';
import {pickLabel, colorArray} from '../helpers/checker';

const AirlineRank = ({airlines, flights, filter, setFilter, filteredFlights, setFilteredFlights, setClickedAirline}) => {
  const [graph, setGraph] = useState();

  useEffect(() => {
    const canvas = document.getElementById('airline-rank');
    const ctx = canvas.getContext("2d");
    
    const colors = colorArray(filteredFlights);
    
    if(graph){
      graph.destroy();
      setGraph();
    }
    var myChart = new Chart(ctx, {
      type: 'bar',
      data: {
          labels: filteredFlights.map(airlineGroup => airlines[airlineGroup.airline]),
          datasets: [{
              label: pickLabel(filter), //make this dynamic
              data: filteredFlights.map(airlineGroup => airlineGroup[filter]),

              //Add in ranges of blue colors based on one blue color, and then calculate opacity ranges based on count
              backgroundColor: colors,
              borderColor: colors,
              borderWidth: 1
          }]
      },
      options: {
          legend: {
            display: false
          },
          onClick: (e, element) => {
            if(element && element[0]){
              const label = element[0]._model.label.replace(" ", "-");
              setClickedAirline(label);
              window.location.href=`/results#${label}`;
            }
          },
          scales: {
              yAxes: [{
                  ticks:{
                    beginAtZero:true
                  }
              }]
          }
      }
    });
    setGraph(myChart);
  }, [filter]);

  return (
    <div>
      {/* <FormControl className={classes.formControl}> */}
        <InputLabel id="filter-label">Airlines Filtered by</InputLabel>
        <Select
          labelId="filter-label"
          id="filter-select"
          onChange={(e) => {
            const newFilter = e.target.value; 
            setClickedAirline(null);
            setFilter(newFilter); 
            setFilteredFlights(filterFlights(flights, newFilter));
            }
          }
          value={filter}
        >
          <MenuItem value={"count"}>Number of Flights</MenuItem>
          <MenuItem value={"depart_delay_avg"}>Average Departure Delay</MenuItem>
          <MenuItem value={"arrival_delay_avg"}>Average Arrival Delay</MenuItem>
        </Select>
        <canvas id="airline-rank" width="400px" height="100px"></canvas>
      {/* </FormControl> */}

    </div>
    )
}

export default AirlineRank;