import React, {useEffect} from 'react';
import Chart from 'chart.js';
import moment from 'moment';

const FlightsPerDayGraph = ({ groupedFlights, airlines, startDate, endDate }) => {

  const datesArray = (start, end) => {
    let dateArr = [];
    for(let currentDate = moment(start); currentDate.format('YYYY-MM-DD') <= end;) {
      dateArr.push(currentDate.format('MM/DD/YYYY'));
      currentDate = currentDate.add(1, 'd');
    }
    return dateArr;
  }

  useEffect(() => {
    let ctx = document.getElementById("flights-per-day").getContext("2d");

    let flightsPerDayGraph = new Chart(ctx, {
      type: "line",
      data: {
        labels: datesArray(startDate, endDate),
        datasets: []
      } 
    });
  });

  return (
    <div>
      <canvas id="flights-per-day" width="400" height="400"></canvas>
    </div>
  );
};

export default FlightsPerDayGraph;
