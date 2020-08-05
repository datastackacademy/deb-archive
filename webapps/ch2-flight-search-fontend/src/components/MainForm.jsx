import React, {Fragment} from 'react';
import {Redirect} from "react-router-dom";
import ls from "local-storage"
import {Button, TextField} from "@material-ui/core";
import {Autocomplete} from '@material-ui/lab';
import {formatDate} from '../helpers/checker';


// Need to add helper function to filter airports if the other input is filled in
const MainForm  = ({preload:{airports, minDate, maxDate}, src, setSrc, dest, setDest, startDate, setStartDate, endDate, setEndDate}) => {
   const handleSubmit = (e) => {
      setSrc(document.getElementById("src-input").value.substring(0,3));
      ls.set("src", document.getElementById("src-input").value.substring(0,3));
      setDest(document.getElementById("dest-input").value.substring(0,3));
      ls.set("dest", document.getElementById("dest-input").value.substring(0,3));
      setStartDate(document.getElementById("start-date").value);
      ls.set("startDate", document.getElementById("start-date").value)
      setEndDate(document.getElementById("end-date").value);
      ls.set("endDate", document.getElementById("end-date").value);
      // return <Redirect to="/results" />
   }

   return (
      <Fragment>
        <form onSubmit={handleSubmit}>
            <Autocomplete 
                id="src-input"
                getOptionLabel={(option) => option.label}
                options = {airports}
                renderInput={(params) => <TextField {...params} label="Departure Airport" variant="outlined" />}
                required
             />
             <br></br>
             <Autocomplete 
                id="dest-input"
                getOptionLabel={(option) => option.label}
                options = {airports}
                renderInput={(params) => <TextField {...params} label="Destination Airport" variant="outlined" />}
                required
             />
             <br></br>
             <TextField
                id="start-date"
                format={"MM/DD/YYYY"}
                label={"Start Date"}
                type={"date"}
                InputLabelProps={{shrink: true}}
                variant="outlined"
                margin="normal"
                required
                
             />
             <TextField
                id="end-date"
                label="End Date"
                type="date"
                InputLabelProps={{shrink: true}}
                variant="outlined"
                margin="normal"
                required
             />
        </form>
        <br></br>
        <Button className="submit-btn" variant="outlined" href="/results" onClick={handleSubmit}>Submit</Button>
        </Fragment>
    )
}

export default MainForm;