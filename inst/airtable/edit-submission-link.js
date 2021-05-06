let tableId = cursor.activeTableId;
let table = base.getTable(tableId);
const koboSurveyID = 'aaztUDtRzb9SpSV7i9iptb';
// Make sure the KOBO_TOKEN is available for the script
const koboToken = process.env.KOBO_TOKEN;
var koboHeaders = new Headers({'Authorization': koboToken});

/* Pick from a table or let the button chose*/
let record = await input.recordAsync('Pick a record', table);

if (record) {    
    let thisRecordID = record.getCellValueAsString('record_id')
    output.text(`Contacting KOBO to generate an edit link for submission with id: ${thisRecordID}...`);
    let response = await remoteFetchAsync(`https://kobo.humanitarianresponse.info/api/v2/assets/${koboSurveyID}/data/${thisRecordID}/edit/?return_url=false&format=json`, {
        method: 'GET',
        headers: koboHeaders
    });
    if (!response.ok) {
        throw `There was a problem generating the link! Try again later. If the problem persists, contact the administrator. Response status: ${response.status}; Response status text: ${response.statusText}. `;
    };
    let r = await response.json();
    if (typeof r.detail === 'undefined'){
        output.markdown(`Click [**here**](${r.url}) to edit this submission or copy paste the link below. \n${r.url}\n\n The link is valid for 30 seconds. After that you'll need to generate a new one. `);
    } else {
        throw `There was a problem with the request: ${r.detail}. Try again later.`
    }
};
