
function updateMetaData(data) {
    
    $("#profile_title").text(data.name+" - "+data.current_team)
    $("#player_position").text(data.position)
    $("#player_height").text(data.height)
    $("#player_weight").text(data.weight)
    $("#player_birthdate").text(data.birth_date)
    $("#player_college").text(data.college)
    $("#player_draft").text("Selected by the "+data.draft_team+" in the "+data.draft_round+"th round ("+data.draft_position+"th overall) of the "+data.draft_year+" NFL Draft")
   
}

function textchanged(newplayer){
    getplayerdata(newplayer)
}

function getplayerdata(newplayer){
    Plotly.d3.json(`/players/${newplayer}`, function(error, metaData) {
        if (error) return console.warn(error);
        updateMetaData(metaData);
    })

    d3.json(`/api/performance/${newplayer}`,function(error,performanceData){
        if(error) return console.warn(error);
        updatePlayerPerformanceChart(performanceData);
    })

    d3.json(`/api/teamperformance/${newplayer}`,function(error,performanceData){
        if(error) return console.warn(error);
        updateTeamPerformanceChart(performanceData);
    })

}

function updatePlayerPerformanceChart(performanceData){
    
    Plotly.restyle("bar", "x", [performanceData.x]);
    Plotly.restyle("bar", "y", [performanceData.y]);
}

function updateTeamPerformanceChart(performanceData){
    
    Plotly.restyle("bubble", "x", [performanceData.x]);
    Plotly.restyle("bubble", "y", [performanceData.y]);
    Plotly.restyle("bubble", 'marker.size', [performanceData.maker.size]);
}

function buildNflGauge(tableid,percentage){
    // Enter a speed between 0 and 180
var level = percentage*1.8;

// Trig to calc meter point
var degrees = 180 - level,
     radius = .5;
var radians = degrees * Math.PI / 180;
var x = radius * Math.cos(radians);
var y = radius * Math.sin(radians);

// Path: may have to change to create a better triangle
var mainPath = 'M -.0 -0.025 L .0 0.025 L ',
     pathX = String(x),
     space = ' ',
     pathY = String(y),
     pathEnd = ' Z';
var path = mainPath.concat(pathX,space,pathY,pathEnd);

var data = [{ type: 'scatter',
   x: [0], y:[0],
    marker: {size: 28, color:'850000'},
    showlegend: false,
    name: 'winning percentage',
    text: level,
    hoverinfo: 'text+name'},
  { values: [50/6, 50/6, 50/6, 50/6, 50/6, 50/6, 50],
  rotation: 90,
  textinfo: 'text',
  textposition:'inside',
  marker: {colors:['rgba(14, 127, 0, .5)', 'rgba(110, 154, 22, .5)',
                         'rgba(170, 202, 42, .5)', 'rgba(202, 209, 95, .5)',
                         'rgba(210, 206, 145, .5)', 'rgba(232, 226, 202, .5)',
                         'rgba(255, 255, 255, 0)']},
  labels: ['151-180', '121-150', '91-120', '61-90', '31-60', '0-30', ''],
  hoverinfo: 'label',
  hole: .5,
  type: 'pie',
  showlegend: false
}];

var layout = {
  shapes:[{
      type: 'path',
      path: path,
      fillcolor: '850000',
      line: {
        color: '850000'
      }
    }],
  height: 450,
  width: 450,
  xaxis: {zeroline:false, showticklabels:false,
             showgrid: false, range: [-1, 1]},
  yaxis: {zeroline:false, showticklabels:false,
             showgrid: false, range: [-1, 1]}
};

Plotly.newPlot(tableid, data, layout);
}


function buildNflCharts(){
    defaultPlayerName = "Tom Brady"
    
    queryplayername = getUrlVars()["playername"]

    if(typeof queryplayername != 'undefined'){
        defaultPlayerName=queryplayername
    }

  


    Plotly.d3.json(`/players/${defaultPlayerName}`, function(error, metaData) {
        if (error) return console.warn(error);
        updateMetaData(metaData);
    })

    d3.json(`/api/performance/${defaultPlayerName}`,function(error,performanceData){
        if(error) return console.warn(error);
        var data = [performanceData];
        var layout = { margin: { t: 30, b: 100 } };
        Plotly.plot("bar", data, layout);
    })

    d3.json(`/api/teamperformance/${defaultPlayerName}`,function(error,performanceData){
        if(error) return console.warn(error);
        var data = [performanceData];
        var layout = {
            margin: { t: 0 },
            hovermode: 'closest',
            xaxis: { title: 'Year' }
        };
        Plotly.plot("bubble", data, layout);
    })

    d3.json(`api/playerwinningpercentage/${defaultPlayerName}`,function(error,performanceData){
        if(error) return console.warn(error);
        buildNflGauge('gauge1',performanceData.home);
        buildNflGauge('gauge2',performanceData.away);
    })

    

}

function getUrlVars()
{
    var vars = [], hash;
    var hashes = window.location.href.slice(window.location.href.indexOf('?') + 1).split('&');
    for(var i = 0; i < hashes.length; i++)
    {
        hash = hashes[i].split('=');
        vars.push(hash[0]);
        vars[hash[0]] = hash[1];
    }
    return vars;
}


function init() {
    //getOptions();
    buildNflCharts();
}
// Initialize the dashboard
init();

