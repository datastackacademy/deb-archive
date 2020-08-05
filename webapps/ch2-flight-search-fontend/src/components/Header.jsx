import React from 'react';


const Header = ({type}) => {
    

    return (
        <a href="/"><img className="logo" src={window.location.origin+"/CrappyFlightsInc.png"} alt="CrappyFlightsInc. Logo" /></a>
    );
}

export default Header;