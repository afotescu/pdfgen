import hummus from 'hummus';
import path from 'path';
import helpers from './helpers';

const transformation = {
    width: 100,
    height: 100,
    proportional: true,
};

const generatePDF = (name, data)  => {

    const pdfWriter = hummus.createWriter(path.join(__dirname, '../', './pdfs', name));

    const fonts = {
        normal: pdfWriter.getFontForFile(path.join(__dirname, './fonts', 'OpenSans-Regular.ttf')),
        bold: pdfWriter.getFontForFile(path.join(__dirname, './fonts', 'OpenSans-Bold.ttf')),
    };

    const page = pdfWriter.createPage(0, 0, 595, 842);
    const content = pdfWriter.startPageContentContext(page);

    let rowCount = 0;
    let seqNumber = 0;
    let bold = false;

    for (let i = 0; i < data.length; i += 1) {
        data[i].field = (data[i].field) ? data[i].field.trim() : '';
        if(data[i].type === 'text') {
            if(!data[i].position_y && data[i].position_y_start && data[i].row_height) {

                if(data[i].font === 'bold' && data[i].wt_sequence !== seqNumber && !bold){
                    seqNumber = data[i].wt_sequence;
                    rowCount += data[i].row_height;
                    bold = true;
                } else if (data[i].font === 'bold' && data[i].wt_sequence !== seqNumber && bold){
                    seqNumber = data[i].wt_sequence;
                    rowCount += (2 * data[i].row_height);
                    bold = false;
                } else if (data[i].font !== 'bold' && data[i].wt_sequence !== seqNumber && bold){
                    seqNumber = data[i].wt_sequence;
                    rowCount += (2 * data[i].row_height);
                    bold = false;
                } else if (data[i].font !== 'bold' && data[i].wt_sequence !== seqNumber && !bold){
                    seqNumber = data[i].wt_sequence;
                    rowCount += data[i].row_height;
                    bold = false;
                } else {
                    seqNumber = data[i].wt_sequence;
                }
                data[i].position_y = data[i].position_y_start - rowCount;
            }
            if(data[i].alignment === 'right' && data[i].position_x) {
                content.writeText(
                    data[i].field,
                    helpers.alignRight(
                        fonts[data[i].font], Number(data[i].size), data[i].field, Number(data[i].position_x)),
                    Number(data[i].position_y),
                    helpers.getOptions(Number(data[i].size), fonts[data[i].font], data[i].color)
                )
            } else if(data[i].position_x){
                content.writeText(
                    data[i].field,
                    data[i].position_x,
                    data[i].position_y,
                    helpers.getOptions(Number(data[i].size), fonts[data[i].font], data[i].color)
                )
            }
        } else if (data[i].type === 'image') {
            content.drawImage(
                Number(data[i].position_x),
                Number(data[i].position_y),
                path.join(__dirname, './templates', data[i].field),
                { transformation }
            )
        } else if (data[i].type === 'line') {
            content.drawPath(
                Number(data[i].position_x),
                Number(data[i].position_y),
                Number(data[i].position_x_end),
                Number(data[i].position_y_end),
            )
        }
    }

    pdfWriter
        .writePage(page)
        .end();
};

export default generatePDF;
