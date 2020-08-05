import moment from 'moment';

const validInputs = (src, dest, start, end) => {
    let output = false;
    if(src && dest && start && end){
        output = true;
    }
    return output;
}

const compareAirports = (state_src, state_dest, airports_src, airports_dest) => {
    let output = false;
    if(state_src === airports_src && state_dest === airports_dest){
        output = true;
    }
    return output;
}

const compareQueries = (obj1, obj2) => {
    let output = false;
    if(obj1.src === obj2.src && obj1.dest === obj2.dest && obj1.start === obj2.start && obj1.end === obj2.end){
        output = true;
    }
    return output;
}

const empty = (obj)=>{
    if(obj){
        if(Object.keys(obj).length === 0){
            return true
        }else{
            return false;
        }
    } else{
        return true;
    }
    
}

const formatDate = (date, formString) => {
    const newMoment = moment(date, "YYYY-MM-DD");
    return newMoment.format(formString);
}

const pickLabel = (currentFilter) => {
    if(currentFilter === "count"){
      return "Number of Flights";
    }
    if(currentFilter === "depart_delay_avg"){
      return "Average Departure Delay";
    }
    if(currentFilter === "arrival_delay_avg"){
      return "Average Arrival Delay";
    }
  } 

const blue = [48,118,172];
const green = [26, 142, 93];
const yellow =[247, 191, 57];

const colorArray = (dataArray, color) => {
    const length = dataArray.length;
    const increment = 0.8/length;
    let colors =[];
    if(color === "y"){
        colors = dataArray.map((group, i) => {
          const incrementValue = (increment * i)+0.2;
          return `rgba(${yellow[0]}, ${yellow[1]}, ${yellow[2]},${incrementValue})`
        });
    } else if (color === "g"){
        colors = dataArray.map((group, i) => {
            const incrementValue = (increment * i)+0.2;
            return `rgba(${green[0]}, ${green[1]}, ${green[2]},${incrementValue})`
          });
    } else{
        colors = dataArray.map((group, i) => {
            const incrementValue = (increment * i)+0.2;
            return `rgba(${blue[0]}, ${blue[1]}, ${blue[2]},${incrementValue})`
          });
    }
    return colors;
}

export {formatDate, validInputs, compareAirports, compareQueries, empty, pickLabel, colorArray}