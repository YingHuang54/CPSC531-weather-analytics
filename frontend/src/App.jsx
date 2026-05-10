import { useState, useEffect } from 'react'

import imgSun from './assets/sun1.svg'
import imgCloud from './assets/cloud1.svg'
import config from './config.json'
import './App.css'

import Plotly from 'react-plotly.js'
const Plot = Plotly.default;

// Frontend
function App() {
  // set the correct url for data fetching
  const mapURL = config.backendAddress + config.mapAPI
  const graphURL = config.backendAddress + config.graphAPI
  const locationMapURL = config.backendAddress + config.locationAPI

  // current states for user input, element interaction, error, loading status, and data received.
  const [hidePanel, setHidePanel] = useState(false);
  const [params, setParams] = useState({
    start_date : '',
    end_date : '',
    level : '',
    agg : '',
    metric: 'TMP',
    id: ''
  });
  const [loading, setLoading] = useState(false)
  const [errorMsg, setErrorMsg] = useState(null);
  const [plotlyData, setPlotlyData] = useState(null);

  // Track the current input
  function handleInput(event){
    setParams({...params, [event.target.name]: event.target.value})
  }

  // Frontend validate user input
  function checkUserInput(params){
    console.log(params)
    const date1 = new Date(params.start_date);
    const date2 = new Date(params.end_date);
    if (params.start_date == '' || params.end_date == ''
      || params.start_date > params.end_date)
      return {error: "Input Error: invalid date"}
    if (params.level == '') 
      return {error: "Input Error: Level can't be empty"}
    if((params.level != 'hourly' && params.agg == '') 
      || (params.level == 'daily' && (params.agg == 'avg_min' || params.agg == 'avg_max')) ){
      return {error: "Input Error: Aggregation method can't be empty"}
    }
    if ((params.level == 'daily' && params.id == '') 
      || params.level == 'hourly'){
      if (params.level == 'daily'
        && (date2 - date1) / (1000 * 60 * 60 * 24) > 31){
        return {error: "Input Error: At most 31 days of daily values for global data"};
      }
      else if (params.level == 'hourly' && params.id == ''
        && (date2 - date1) / (1000 * 60 * 60 * 24) > 2){
          return {error: "Input Error: At most 2 days of hourly data for global data"};
      }
      else if (params.level == 'hourly' && params.id != ''
        && (date2 - date1) / (1000 * 60 * 60 * 24) > 31){
          return {error: "Input Error: At most 31 days of hourly data for a location"}
      }
    }
    return {error: null}
  }

  // Handle form submit, call the appropriate backend API
  const handleAggSubmit = async (event) => {
    event.preventDefault()
    setLoading(true)
    setPlotlyData(null)
    setErrorMsg(null)
    // validate input
    params.id = params.id.trim()
    const invalidUserInput = checkUserInput(params);
    console.log("User input:" + (invalidUserInput["error"] == null))
    if ('error' in invalidUserInput && invalidUserInput["error"] != null){
      setErrorMsg(invalidUserInput.error)
      setLoading(false)
      return
    }
    // fetch data from backend
    try{
      console.log(params);
      const data = JSON.stringify(params);
      const apiURL = (params["id"] != '') ? graphURL : mapURL
      console.log("sending to " + apiURL)
      const response = await fetch(apiURL, {
        method: "POST",
        headers: {'Content-Type': "application/json"},
        body: data
      });
      if (!response.ok) throw new Error("Server Error")
      else {
        const result = await response.json()
        if ('error' in result){
          setErrorMsg(result.error)
          setLoading(false)
        }else{
          setPlotlyData(result)
        }
      }
    } catch(err) {
      setErrorMsg(err.message)
    } finally {
      setLoading(false)
    }
  }

  // handle location map button click
  const handleLocationMap = async (event) => {
    setLoading(true)
    setPlotlyData(null)
    setErrorMsg(null)
    try{
      // fetch data from backend
      const response = await fetch(locationMapURL, {
        method: "GET"
      });
      if (!response.ok) throw new Error("Server Error")
      else {
        const result = await response.json()
        if ('error' in result){
          setErrorMsg(result.error)
          setLoading(false)
        }else{
          setPlotlyData(result)
        }
      }
    } catch(err) {
      setErrorMsg(err.message)
    } finally {
      setLoading(false)
    }
  }

  // Frontend display 
  return (
    <div className='appContainer'>
      <header>
        <h1>Weather Data</h1>
      </header>
      <main className='main_content'>
        <section className = {`sidePanel ${hidePanel? 'hide':''}`}>
          <form className = 'inputForm'>
            <fieldset disabled = {loading}>
            <div className='formElement'>
              <h4>Available Data From 2014-01-01 to 2023-12-31</h4>
            </div>

            <div className='formElement'>
              <label
              title='if level = year, only the year be taken in.\n
              if level = month, only the year and month will be taken in. \n'
              > Start Date
              </label>
              <input type='date' className='field' name='start_date'
              onChange={handleInput}
              ></input>
            </div>

            <div className='formElement'>
              <label
              title='if level = year, only the year be taken in.\n
              if level = month, only the year and month will be taken in. \n'
              > End Date
              </label>
              <input type='date' className='field' name='end_date'
              onChange={handleInput}
              ></input>
            </div>

            <div className='formElement'>
              <label >
                Level
              </label>
              <select className='field' name='level'
                onChange={handleInput}
                >
                  <option value=''></option>
                  <option value='hourly'>Hourly</option>
                  <option value='daily'>Daily</option>
                  <option value='monthly'>Monthly</option>
                  <option value='yearly'>Yearly</option>
                </select>
            </div>

            <div className='formElement'>
              <label >
                Aggregation Method
              </label>
              
              <select className='field' name='agg'
                onChange={handleInput}
                >
                  <option value=''></option>
                  { params.level != 'hourly' && (
                    <>
                    <option value='avg'>Average</option>
                    <option value='min'>Minimum</option>
                    <option value='max'>Maximum</option> 
                    </>
                  )}
                  {(params.level == 'monthly' || params.level == 'yearly')
                    && (
                      <>
                      <option value='avg_min'>Average Daily Minimum</option>
                      <option value='avg_max'>Average Daily Maximum</option>
                      </>
                  )}
                </select> 
              
            </div>

            <div className='formElement'>
              <label >
                Metric
              </label>
              <select className='field'
                name = 'metric'
                onChange={handleInput}
                >
                  <option value='TMP'>Temperature</option>
                  <option value='DEW'>Dew Point</option>
                  <option value='WIND'>Wind Rate</option>
                  <option value='SLP'>Sea Level Pressure</option>
                </select>
            </div>

            <div className='formElement'>
              <label
              title='id of the weather station in the weather station map.'
              > Weather Station ID
              </label>
              <input type='text' className='field' 
              name='id' placeholder='#####-####'
              onChange={handleInput}
              ></input>
            </div>

            <div className='formElement'>
              <button onClick={handleLocationMap}>
                Check Weather Station Map
              </button>
            </div>
            
            <div className='formElement'>
              <button type='submit' id='aggregateBtn'
              onClick={handleAggSubmit}>
              {params.id.length > 0? 'Graph' : 'Map' }
              </button>
            </div>

            </fieldset>
          </form>

          <button className ='sideBtn' onClick={(e) => setHidePanel(!hidePanel)}>
            <img id = 'sideBtnImgOpen' src={imgSun}/>
            <img id = 'sideBtnImgClose' src = {imgCloud}/>
          </button>
        </section>
        <section className='graphContainer'>
          {
            errorMsg &&
            (<div className='messageDisplay'>
              {errorMsg}
            </div>)
          }
          {
            loading && (
              <div className='messageDisplay'>
                Loading...
              </div>
            )
          }
          <div id="plotDiv"></div>
          { 
            plotlyData &&
            (<Plot key = {JSON.stringify(plotlyData.data)}
              data = {plotlyData.data}
              layout = {{
                ...plotlyData.layout,
                autosize: true,
                margin: {l: 0, r: 0, t: 0, b: 0}
              }}
              frames = {plotlyData.frames}
              useResizeHandler = {true}
              style={{width:"95%", height:"98%"}} 
            />)
          }
        </section>
      </main>
    </div>
  )
}

export default App
