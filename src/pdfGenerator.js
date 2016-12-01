import hummus from 'hummus';
import path from 'path';
import helpers from './helpers';

const generatePDF = (name, data)  => {

    const pdfWriter = hummus.createWriter(path.join(__dirname, '../', './pdfs', name));

    const fonts = {
        openSans: pdfWriter.getFontForFile(path.join(__dirname, './fonts', 'OpenSans-Regular.ttf')),
        openSansBold: pdfWriter.getFontForFile(path.join(__dirname, './fonts', 'OpenSans-Bold.ttf')),
    };

    const page = pdfWriter.createPage(0, 0, 595, 842);
    const content = pdfWriter.startPageContentContext(page);

    for (let i = 0; i < data.length; i += 1) {
        if(data[i].type === 'text') {
            if(data[i].alignment === 'right') {
                content.writeText(
                    data[i].field,
                    helpers.alignRight(
                        fonts[data[i].font], Number(data[i].size), data[i].field, Number(data[i].position_x)),
                    Number(data[i].position_y),
                    helpers.getOptions(Number(data[i].size), fonts[data[i].font], data[i].color)
                )
            } else {
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
                path.join(__dirname, './templates', data[i].field)
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
