// const PROCESSING_STATS_API_URL = "http://64.23.146.135/processing/stats"
// const ANALYZER_API_URL = {
//     stats: "http://64.23.146.135/analyzer/stats",
//     temperature: "http://64.23.146.135/analyzer/forest_fire/temperatures",
//     airquality: "http://64.23.146.135/analyzer/forest_fire/airquality",
//     randomTemp: "http://64.23.146.135/analyzer/temperature/random"  // ADD THIS
// } use when server

// For LOCAL testing
const PROCESSING_STATS_API_URL = "http://localhost/processing/stats"
const ANALYZER_API_URL = {
    stats: "http://localhost/analyzer/stats",
    temperature: "http://localhost/analyzer/forest_fire/temperatures",
    airquality: "http://localhost/analyzer/forest_fire/airquality",
    randomTemp: "http://localhost/analyzer/temperature/random"
}


// This function fetches and updates the general statistics
const makeReq = (url, cb) => {
    fetch(url)
        .then(res => res.json())
        .then((result) => {
            console.log("Received data: ", result)
            cb(result);
        }).catch((error) => {
            updateErrorMessages(error.message)
        })
}

const makeReqQuery = (url, query, cb) => {
    fetch(url + '?' + new URLSearchParams(query).toString())
        .then(res => res.json())
        .then((result) => {
            console.log("Received data: ", result)
            cb(result);
        }).catch((error) => {
            updateErrorMessages(error.message)
        })
}

const updateCodeDiv = (result, elemId) => document.getElementById(elemId).innerText = JSON.stringify(result)

const getLocaleDateStr = () => (new Date()).toLocaleString()

let max_passenger = 0
let max_train = 0

const getStats = () => {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr()
    
    makeReq(PROCESSING_STATS_API_URL, (result) => updateCodeDiv(result, "processing-stats"))
    makeReq(ANALYZER_API_URL.stats, (result) => {
        const max_temp = result.num_temperature_readings || 0;
        console.log(max_temp)
        const max_air = result.num_airquality_readings || 0;
        console.log(max_air)
        updateCodeDiv(result, "analyzer-stats")
        
        if (max_temp > 0) {
            makeReqQuery(ANALYZER_API_URL.temperature, { index: Math.floor(Math.random() * max_temp) }, (result) => updateCodeDiv(result, "event-temperature"))
        }
        if (max_air > 0) {
            makeReqQuery(ANALYZER_API_URL.airquality, { index: Math.floor(Math.random() * max_air) }, (result) => updateCodeDiv(result, "event-airquality"))
        }
    })
}

const updateErrorMessages = (message) => {
    const id = Date.now()
    console.log("Creation", id)
    msg = document.createElement("div")
    msg.id = `error-${id}`
    msg.innerHTML = `<p>Something happened at ${getLocaleDateStr()}!</p><code>${message}</code>`
    document.getElementById("messages").style.display = "block"
    document.getElementById("messages").prepend(msg)
    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`)
        if (elem) { elem.remove() }
    }, 7000)
}

const setup = () => {
    getStats()

    // setInterval(() => getStats(), 2000) 
    setInterval(() => getStats(), 4000) //DEFAULT
    // setInterval(() => getStats(), 8000) 
    // setInterval(() => getStats(), 10000) 
}







document.addEventListener('DOMContentLoaded', setup)
