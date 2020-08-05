import React,{ useState, useEffect} from 'react';
import {BrowserRouter, Route, Switch} from 'react-router-dom';
import {trackPormise} from 'react-promise-tracker';
import ls from 'local-storage';
import moment from 'moment';
import './App.css';

import Search from './pages/Search';
import Results from './pages/Results';
import Header from './components/Header';
import {fetchPreload, formatAirportOptions} from './helpers/apiCalls';

function App() {
  const [preload, setPreload] = useState({airports:[], minDate:"", maxDate:""});
  const [src, setSrc] = useState(ls.get("src"));
  const [dest, setDest] = useState(ls.get("dest"));
  const [startDate, setStartDate] = useState(ls.get("startDate"));
  const [endDate, setEndDate] = useState(ls.get("endDate"));
  const [noFlights, setNoFlights] = useState(ls.get("noFlights")? ls.get("noFlights"):false);

  useEffect(() => {
    if(!ls.get('preload')){
      fetchPreload("http://localhost:5000/")
      .then(res => res.json())
      .then(data => {
        let return_obj = {}
        const airports = JSON.parse(data.airports)
        return_obj.airports = airports.map( airport =>
          formatAirportOptions(airport));
        return_obj.minDate = moment(data.min_date, "ddd, DD MMM YYYY kk:mm:ss zz").format("YYYY-MM-DD");
        return_obj.maxDate = moment(data.max_date, "ddd, DD MMM YYYY kk:mm:ss zz").format("YYYY-MM-DD");
        ls.set('preload', return_obj);
        setPreload(return_obj);
      })
    } else{
      setPreload(ls.get('preload'))
    }    
  }, [])

  return (
    <div className="App">
      <header>
        <Header />
        <BrowserRouter>
          <Switch>
            <Route exact path="/">
              <Search noFlights={noFlights} preload={preload} src={src} setSrc={setSrc} dest={dest} setDest={setDest} startDate={startDate} setStartDate={setStartDate} endDate={endDate} setEndDate={setEndDate}/>
            </Route>
            <Route exact path="/results">
              <Results noFlights={noFlights} setNoFlights={setNoFlights} preload={preload} src={src} dest={dest} startDate={startDate} endDate={endDate}/>
            </Route>
          </Switch>
        </BrowserRouter>
      </header>
      <main>
      </main>
    </div>
  )
}

export default App;
