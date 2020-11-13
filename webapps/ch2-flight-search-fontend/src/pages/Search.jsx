import React from 'react';
import MainForm from '../components/MainForm';
import { formatDate } from '../helpers/checker'
import ls from "local-storage";



const Search = (props) => {
  const previousValidQuery = ls.get("previousQuery")? ls.get("previousQuery") : "";

  const handleClick = () => {
    props.setSrc(previousValidQuery.src);
    ls.set("src", previousValidQuery.src);
    props.setDest(previousValidQuery.dest);
    ls.set("dest", previousValidQuery.dest);
    props.setStartDate(previousValidQuery.start);
    ls.set("startDate", previousValidQuery.start);
    props.setEndDate(previousValidQuery.end);
    ls.set("endDate", previousValidQuery.end);

    window.location.href = '/results'
  }

  return (
    <div className="page-container">
      {previousValidQuery.src && previousValidQuery.dest && previousValidQuery.start && previousValidQuery.end && <em className="previous-search" onClick={handleClick}>Previous Search: {previousValidQuery.src} to {previousValidQuery.dest} from {formatDate(previousValidQuery.start, "MM/DD/YYYY")} to {formatDate(previousValidQuery.end, "MM/DD/YYYY")}</em>}
      <br></br>
      <MainForm {...props} />
    </div>)
}

export default Search;