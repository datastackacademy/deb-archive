import moment from 'moment';

const datesArray = (start, end) => {
  let dateArr = [];
  for(let currentDate = moment(start); currentDate.format('YYYY-MM-DD') <= end;) {
    dateArr.push(currentDate.format('MM/DD/YYYY'));
    currentDate = currentDate.add(1, 'd');
  }
  return dateArr;
}

const addZeroIfNoFlight = (counts, dates) => {
  let returnCounts = [];
  dates.forEach(date => {
    let formattedDate = moment(date).format("YYYY-MM-DD")
    !counts[formattedDate]? returnCounts.push(0): returnCounts.push(counts[formattedDate]);
  })
  return returnCounts;
}

const graphData = (groupedFlights, airlines, graphDates) => {

  const colors = [
  'rgba(10, 33, 51, 1)',
  'rgba(44, 74, 99, 1)',
  'rgba(11, 93, 156, 1)',
  'rgba(48, 118, 172, 1)',
  'rgba(43, 146, 224, 1)',
  'rgba(62, 157, 230, 1)', 
  'rgba(64, 179, 131, 1)',
  'rgba(26, 142, 93, 1)',
  'rgba(34, 89, 65, 1)',
  'rgba(6, 48, 31, 1)'];

  return groupedFlights.map((airlineFlights, i) => {
    let returnObj = {};
    returnObj.label = airlines[airlineFlights.airline];
    returnObj.data = addZeroIfNoFlight(airlineFlights.flights_per_day_count, graphDates);
    returnObj.fill = false;
    returnObj.borderColor = colors[i];
    returnObj.pointBackgroundColor = colors[i];
    returnObj.lineTension = 0;
    return returnObj;
  });
}

export {datesArray, graphData};