//todo fix property names to actual
import React from 'react';
import {formatDate} from '../helpers/checker';

const FlightRow = ({flight, setCurrentFlight, setDisplayModal}) => {
    const handleClick = () => {
        setCurrentFlight(flight);
        setDisplayModal(true);
    }
    return (
        <tr onClick={handleClick} style={{cursor:"pointer"}}>
            <td>{formatDate(flight.flight_date, "MM/DD/YYYY")}</td>
            <td>{flight.flight_number}</td>
            <td>{flight.departure_delay}</td>
            <td>{flight.arrival_delay}</td>
        </tr>
    )
}

export default FlightRow;