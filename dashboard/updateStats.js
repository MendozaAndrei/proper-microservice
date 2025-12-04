// API URLs - Update for production
const PROCESSING_STATS_API_URL = "http://localhost/processing/stats"
const ANALYZER_API_URL = {
    stats: "http://localhost/analyzer/stats",
    temperature: "http://localhost/analyzer/forest_fire/temperatures",
    airquality: "http://localhost/analyzer/forest_fire/airquality",
    randomTemp: "http://localhost/analyzer/temperature/random"
}

// const PROCESSING_STATS_API_URL = "http://64.23.146.135/processing/stats"
// const ANALYZER_API_URL = {
//     stats: "http://64.23.146.135/analyzer/stats",
//     temperature: "http://64.23.146.135/analyzer/forest_fire/temperatures",
//     airquality: "http://64.23.146.135/analyzer/forest_fire/airquality",
//     randomTemp: "http://64.23.146.135/analyzer/temperature/random"
// }
// const HEALTH_API_URL = "http://64.23.146.135/health/health"  // Production URL
const HEALTH_API_URL = "http://localhost/health/health"  // DEPLOYMENT CODE. CHANGE THIS WHEN DEPLOYING TO SERVER

// Existing functions
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


// This gets the HEALTH STATUS from the health endoiints of each thingers
const getHealthStatus = () => {
    makeReq(HEALTH_API_URL, (result) => {
        updateHealthStatus('receiver', result.receiver)
        
        updateHealthStatus('storage', result.storage)
        
        updateHealthStatus('processing', result.processing)
        
        updateHealthStatus('analyzer', result.analyzer)
        
        // Update last update time
        if (result.last_update) {
            const lastUpdate = new Date(result.last_update)
            const now = new Date()
            const secondsAgo = Math.floor((now - lastUpdate) / 500)
            document.getElementById('health-last-update').innerText = `${secondsAgo} seconds ago`
        }
    })
}

const updateHealthStatus = (serviceName, status) => {
    const element = document.getElementById(`health-${serviceName}`)
    if (element) {
        element.innerText = status
        
        // Remove all status classes
        element.classList.remove('status-running', 'status-down', 'status-unknown')
        
        // Add appropriate class based on status
        if (status === 'Running') {
            element.classList.add('status-running')
        } else if (status === 'Down') {
            element.classList.add('status-down')
        } else {
            element.classList.add('status-unknown')
        }
    }
}

const getStats = () => {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr()
    
    makeReq(PROCESSING_STATS_API_URL, (result) => updateCodeDiv(result, "processing-stats"))
    makeReq(ANALYZER_API_URL.stats, (result) => {
        const max_temp = result.num_temperature_readings || 0;
        const max_air = result.num_airquality_readings || 0;
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
    getHealthStatus()  // ADD THIS - Initial call

    // Update stats every 4 seconds
    setInterval(() => getStats(), 4000)
    
    // Update health status every 4 seconds (can be different interval if you want)
    setInterval(() => getHealthStatus(), 4000)  // ADD THIS
}

document.addEventListener('DOMContentLoaded', setup)