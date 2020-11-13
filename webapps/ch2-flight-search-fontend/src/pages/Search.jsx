import React from 'react';
import MainForm from '../components/MainForm';
import { formatDate } from '../helpers/checker'
import ls from "local-storage";



const Search = (props) => {
  const previousValidQuery = ls.get("previousQuery")? ls.get("previousQuery") : "";

  return (
    <div className="page-container">
      {previousValidQuery.src && previousValidQuery.dest && previousValidQuery.start && previousValidQuery.end && <em className="previous-search" onClick={() => window.location.href = '/results'}>Previous Search: {previousValidQuery.src} to {previousValidQuery.dest} from {formatDate(previousValidQuery.start, "MM/DD/YYYY")} to {formatDate(previousValidQuery.end, "MM/DD/YYYY")}</em>}
      <br></br>
      <MainForm {...props} />
    </div>)
}

export default Search;