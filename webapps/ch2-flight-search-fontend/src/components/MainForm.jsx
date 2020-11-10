import React, {Fragment, useState} from 'react';
import ls from "local-storage"
import {Button, TextField} from "@material-ui/core";
import {Autocomplete} from '@material-ui/lab';
import { KeyboardDatePicker, MuiPickersUtilsProvider } from '@material-ui/pickers';
import MomentUtils from '@date-io/moment';
import {formatDate} from '../helpers/checker';


// Need to add helper function to filter airports if the other input is filled in
const MainForm  = ({preload:{airports, minDate, maxDate}, src, setSrc, dest, setDest, startDate, setStartDate, endDate, setEndDate}) => {
  
  const [inputStartDate, setInputStartDate] = useState("2018-01-01");
  const [inputEndDate, setInputEndDate] = useState("2018-01-01");

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

   return (
      <Fragment>
        <form onSubmit={handleSubmit} action="/results">
            <Autocomplete 
                id="src-input"
                getOptionLabel={(option) => option.label}
                options = {airports}
                renderInput={(params) => <TextField {...params} label="Departure Airport" variant="outlined" required={true} />}
                
             />
             <br></br>
             <Autocomplete 
                id="dest-input"
                getOptionLabel={(option) => option.label}
                options = {airports}
                renderInput={(params) => <TextField {...params} label="Destination Airport" variant="outlined" required={true} />}
                
             />
             <br></br>
             <MuiPickersUtilsProvider utils={MomentUtils}>
                <KeyboardDatePicker
                  disableToolbar
                  autoOk
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
                  maxDate={maxDate}
                  onChange={e => setInputStartDate(formatDate(e, "YYYY-MM-DD"))}
                />
                <KeyboardDatePicker
                  disableToolbar
                  autoOk
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
                  maxDate={maxDate}
                  onChange={e => setInputEndDate(formatDate(e, "YYYY-MM-DD"))}
                />  
               </MuiPickersUtilsProvider>
             <br></br>
             <Button type="submit" className="submit-btn" variant="outlined" onSubmit={handleSubmit}>Submit</Button>
        </form>
        </Fragment>
    );
}

export default MainForm;