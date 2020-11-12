import React, {Fragment, useState, useEffect} from 'react';
import ls from "local-storage";
import {Button, TextField} from "@material-ui/core";
import {Alert, Autocomplete} from '@material-ui/lab';
import { KeyboardDatePicker, MuiPickersUtilsProvider } from '@material-ui/pickers';
import MomentUtils from '@date-io/moment';
import {formatDate} from '../helpers/checker';


// Need to add helper function to filter airports if the other input is filled in
const MainForm  = ({preload:{airports, minDate, maxDate}, src, setSrc, dest, setDest, startDate, setStartDate, endDate, setEndDate}) => {
  
  const [inputStartDate, setInputStartDate] = useState("2018-01-01");
  const [inputEndDate, setInputEndDate] = useState("2018-01-01");
  const [hasDateSelectionError, setHasDateSelectionError] = useState(false);

   const handleSubmit = (e) => {
      setSrc(document.getElementById("src-input").value.substring(0,3));
      ls.set("src", document.getElementById("src-input").value.substring(0,3));
      setDest(document.getElementById("dest-input").value.substring(0,3));
      ls.set("dest", document.getElementById("dest-input").value.substring(0,3));
      setStartDate(inputStartDate);
      ls.set("startDate", inputStartDate);
      setEndDate(inputEndDate);
      ls.set("endDate", inputEndDate);
   }

  const filterDestOptions = (options, { inputValue }) => {
    const srcSelection = document.getElementById("src-input").value;
    const filteredAirports = options.filter(e => (e.label !== srcSelection && e.label.toLowerCase().includes(inputValue.toLowerCase())));
    return filteredAirports;
  }
   useEffect(() => {
    Date.parse(inputEndDate) < Date.parse(inputStartDate)? setHasDateSelectionError(true): setHasDateSelectionError(false)
   }, [inputStartDate, inputEndDate])

   return (
      <Fragment>
        <form onSubmit={handleSubmit} action="/results">
            <Autocomplete 
                id="src-input"
                disableClearable
                getOptionLabel={(option) => option.label}
                options = {airports}
                renderInput={(params) => <TextField {...params} label="Departure Airport" variant="outlined" required={true} />}
             />
             <br></br>
             <Autocomplete 
                id="dest-input"
                disableClearable
                getOptionLabel={(option) => option.label}
                options = {airports}
                filterOptions={filterDestOptions}
                renderInput={(params) => <TextField {...params} label="Destination Airport" variant="outlined" required={true} />}
             />
             <br></br>
             {hasDateSelectionError && <Alert severity="error">End date selection cannot be before start date selection</Alert>}
             <MuiPickersUtilsProvider utils={MomentUtils}>
                <KeyboardDatePicker
                  autoOk
                  error={hasDateSelectionError || inputStartDate > maxDate || inputStartDate < minDate}
                  value={inputStartDate}
                  id="start-date"
                  variant="inline"
                  format="MM/DD/yyyy"
                  inputVariant="outlined"
                  label="Start Date"
                  InputLabelProps={{shrink: true}}
                  margin="normal"
                  required={true}
                  minDate={minDate}
                  minDateMessage={`There is no flight data before ${minDate}`}
                  maxDate={maxDate}
                  maxDateMessage={`There is no flight data after ${maxDate}`}
                  onChange={e => setInputStartDate(formatDate(e, "YYYY-MM-DD"))}
                />
                <KeyboardDatePicker
                  autoOk
                  error={hasDateSelectionError||inputEndDate > maxDate|| inputEndDate < minDate}
                  value={inputEndDate}
                  id="end-date"
                  variant="inline"
                  format="MM/DD/yyyy"
                  inputVariant="outlined"
                  label="End Date"
                  InputLabelProps={{shrink: true}}
                  margin="normal"
                  required={true}
                  minDate={minDate}
                  minDateMessage={`There is no flight data before ${minDate}`}
                  maxDate={maxDate}
                  maxDateMessage={`There is no flight data after ${maxDate}`}
                  onChange={e => setInputEndDate(formatDate(e, "YYYY-MM-DD"))}
                />  
               </MuiPickersUtilsProvider>
             <br></br>
             <Button disabled={hasDateSelectionError} type="submit" className="submit-btn" variant="outlined" color="primary" onSubmit={handleSubmit}>Submit</Button>
        </form>
        </Fragment>
    );
}

export default MainForm;