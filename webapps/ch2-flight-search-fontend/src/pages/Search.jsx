import React from 'react';
import MainForm from '../components/MainForm';
import {formatDate} from '../helpers/checker'



const Search = (props) => {
    return (
        <div className="page-container">
            {console.log(props.noFlights)}
            {props.src&&props.dest&&props.startDate&&props.endDate&&!props.noFlights&&<em className="previous-search" onClick={() => window.location.href='/results'}>Previous Search: {props.src} to {props.dest} from {formatDate(props.startDate, "MM/DD/YYYY")} to {formatDate(props.endDate, "MM/DD/YYYY")}</em>}
            <br></br>
            <MainForm {...props}/>
        </div>)
}

export default Search;