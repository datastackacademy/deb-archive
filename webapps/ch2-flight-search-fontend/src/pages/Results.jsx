import React,{Fragment, useEffect, useState} from 'react';
import ls from "local-storage";
import {Button, Modal} from "@material-ui/core";

import AirlineRank from "../components/AirlineRank";
import AirlineContainer from '../components/AirlineContainer';
import FlightModal from '../components/FlightModal';
import Map from '../components/Map';

import { fetchAirportInfo, fetchFlights, filterQueryDate, fetchAirlines, airlineDictionary } from '../helpers/apiCalls';
import {pickLabel, validInputs, compareAirports, compareQueries, empty, formatDate} from '../helpers/checker';
import {groupByAirline, findAllAirlines, filterFlights,rankCount} from '../helpers/filterRank';


const Results = ({noFlights, setNoFlights, preload, src, dest, endDate, startDate}) => {
    //airport info for map pins
    const [airportInfo, setAirportInfo] = useState(ls.get("airportInfo"));
    //flights returned from query
    const [flights, setFlights] = useState(ls.get("flights"));
    //previously fetched query
    const [previousQuery, setPreviousQuery] = useState(ls.get("previousQuery"));
    //current filter setting
    const [filter, setFilter] = useState("count");
    //flights filtered by filter set in filter
    const [filteredFlights, setFilteredFlights] = useState(flights&&filter?filterFlights(flights,filter):[])
    //error if incorrect info was submitted in form
    const [errorMessage, setErrorMessage] = useState(false);
    //currently clicked flight
    const [currentFlight, setCurrentFlight] = useState(null);
    //modal for flights display status
    const [displayModal, setDisplayModal] = useState(false);
    //airline names
    const [airlines, setAirlines] = useState(null);
    const [clickedAirline, setClickedAirline] = useState(null);


    const NoFlights = () => {
        return (
            <div className="no-flights">
                <em>
                    Looks like there aren't any flights from {src} to {dest} from {formatDate(startDate, "MM/DD/YYYY")} to {formatDate(endDate, "MM/DD/YYYY")}. Want to try a different search?
                </em>
                <br/>
                <br/>
                <Button variant="outlined" color="primary" href="/">Back to Search</Button>
            </div>
        )
    }

    const Loading = () => {
        return (
            <div className="loading">
                <h1>Loading<span className="animate-flicker">...</span></h1>
            </div>
        )
    }
    const ErrorMessage = () =>{
        return (
            <div>
                <p>It seems like you have not made a valid search. Please go back to the main page and refine your search.
                </p> 
                <Button variant="outlined" color="primary" href="/">Back to Search</Button>
            </div>
        );
    }

    const Title = () => {

       return (
            <div>
                <h1>
                    {airportInfo.src.iata} <img src={window.location.origin+'/plane.svg'} alt="plane-img" /> {airportInfo.dest.iata} 
                </h1>
                <h2>
                    {formatDate(startDate, "MM/DD/YYYY")} - {formatDate(endDate, "MM/DD/YYYY")}
                </h2>
            </div>
       )
    }

    const modalRender = () =>  {
        return (
            displayModal && 
                <Modal 
                    open={() => setDisplayModal(true)}
                    onClose={() => setDisplayModal(false)}
                    aira-labelledby="flight-information"
                >
                    {<FlightModal airlines={airlines} filter={filter} filteredFlights={filteredFlights} flight={currentFlight} allFlights={flights} setDisplayModal={setDisplayModal} />}
                </Modal>
            )
    }

    useEffect(() =>{
        const currentQuery = {"src": src, "dest": dest, "start": startDate, "end": endDate};
        ls.set("noFlights", false);
        setNoFlights(false);
        if(validInputs(src,dest,startDate,endDate)){
            setErrorMessage(false);
            fetchAirportInfo('http://localhost:5000/', src, dest)
                .then(res => res.json())
                .then(res => {
                    setAirportInfo(res);
                    ls.set("airportInfo", res);
                });
            fetchFlights('http://localhost:5000/', src, dest, startDate, endDate)
            .then(res => res.json())
            .then(res => {
                const f = res.flights;
                console.log(f)

                if(f.length > 0){
                    setFlights(f);
                    
                    const ff = filterFlights(f,filter)
                    setFilteredFlights(ff);

                    setPreviousQuery(currentQuery);
                    ls.set("previousQuery", currentQuery);

                    const allAirlines = findAllAirlines(f);
                    return fetchAirlines('http://localhost:5000/',allAirlines);
                } else{
                    ls.set("noFlights", true);
                    setNoFlights(true);
                    return "empty";
                }
               
            })
            .then(res => {
                if(res !== "empty"){
                    return res.json();
                }else{
                    return "empty";
                }}
            )
            .then(res => {
                if(res !== "empty"){
                    const currentAirlines = airlineDictionary(res);
                    setAirlines(currentAirlines);
                } 
            });
        }else{
            setErrorMessage(true);
        }

    }, [])

    return (
        <div className="page-container">
        {
            errorMessage ? 
                ( 
                    <ErrorMessage />
                ) :
                <Fragment>
                    {noFlights? <NoFlights /> :
                    (<Fragment>
                        {modalRender()}
                        <div className="results">
                                {(airportInfo&&flights&&filteredFlights&&airlines)?
                                    (  
                                        <Fragment>
                                            <Title />
                                            <Map airportInfo={airportInfo} />
                                            <AirlineRank setClickedAirline={setClickedAirline} airlines={airlines} flights={flights} filter={filter} setFilter={setFilter} filteredFlights={filteredFlights} setFilteredFlights={setFilteredFlights} setClickedAirline={setClickedAirline}/>
                                            <div className="airline-containers">
                                                <h1>Flights by Airline</h1>
                                                <p><emphasis>Airlines sorted by {filter&&pickLabel(filter).toLowerCase()}</emphasis></p>
                                                {filteredFlights.map((airlineGroup, i) => {
                                                    const sectionName = airlines[airlineGroup.airline].replace(" ","-");
                                                    if(sectionName === clickedAirline){
                                                        return <AirlineContainer key={`airlineContainer-${i}`} rank={i+1} expanded={true} airlines={airlines} airlineGroup={airlineGroup} currentFlight={currentFlight} setCurrentFlight={setCurrentFlight} clickedAirline={clickedAirline} setClickedAirline={setClickedAirline} setDisplayModal={setDisplayModal} sectionName={sectionName}/>
                                                    }else{
                                                        return <AirlineContainer key={`airlineContainer-${i}`} rank={i+1} expanded={false} airlines={airlines} airlineGroup={airlineGroup} currentFlight={currentFlight} setCurrentFlight={setCurrentFlight} clickedAirline={clickedAirline} setClickedAirline={setClickedAirline} setDisplayModal={setDisplayModal} sectionName={sectionName}/>
                                                    }
                                                    
                                                })
                                                }
                                            </div>
                                        </Fragment>
                                    ) :
                                    <Loading />}
                    </div>
                    </Fragment>)}
                </Fragment>
                
        }
        </div>
        
    )
}

export default Results;
