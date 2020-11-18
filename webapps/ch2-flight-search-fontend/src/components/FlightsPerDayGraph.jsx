import React, { useEffect} from 'react';
import Chart from 'chart.js';
import { datesArray, graphData } from '../helpers/flightsPerDay';

const FlightsPerDayGraph = ({ groupedFlights, airlines, startDate, endDate }) => {
  const graphDates = datesArray(startDate, endDate);
  const gData = graphData(groupedFlights, airlines, graphDates);

  useEffect(() => {
    let ctx = document.getElementById("flights-per-day").getContext("2d");

    // eslint-disable-next-line
    let flightsPerDayGraph = new Chart(ctx, {
      type: "line",
      data: {
        labels: graphDates,
        datasets: gData
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
  // eslint-disable-next-line
  }, []);

  return (
    <div>
      <canvas id="flights-per-day" width="400" height="400"></canvas>
    </div>
  );
};

export default FlightsPerDayGraph;
