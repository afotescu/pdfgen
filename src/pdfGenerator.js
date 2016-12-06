import hummus from 'hummus';
import path from 'path';
import config from './config';
import helpers from './helpers';

const transformation = {
    width: 100,
    height: 100,
    proportional: true,
};

const generatePDF = (filePath, info, logo) => {
    const pdfWriter = hummus.createWriter(path.join(filePath));
    const data = Object.assign({}, info);
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
        if (data[i].type === 'text') {
            if (!data[i].position_y && data[i].position_y_start && data[i].row_height) {
                if (data[i].font === 'bold' && data[i].wt_sequence !== seqNumber && !bold) {
                    seqNumber = data[i].wt_sequence;
                    rowCount += data[i].row_height;
                    bold = true;
                } else if (data[i].font === 'bold' && data[i].wt_sequence !== seqNumber && bold) {
                    seqNumber = data[i].wt_sequence;
                    rowCount += (2 * data[i].row_height);
                    bold = false;
                } else if (data[i].font !== 'bold' && data[i].wt_sequence !== seqNumber && bold) {
                    seqNumber = data[i].wt_sequence;
                    rowCount += (2 * data[i].row_height);
                    bold = false;
                } else if (data[i].font !== 'bold' && data[i].wt_sequence !== seqNumber && !bold) {
                    seqNumber = data[i].wt_sequence;
                    rowCount += data[i].row_height;
                    bold = false;
                } else {
                    seqNumber = data[i].wt_sequence;
                }
                data[i].position_y = data[i].position_y_start - rowCount;
            }
            if (data[i].alignment === 'right' && data[i].position_x) {
                helpers.printText(
                    content,
                    fonts[data[i].font],
                    Number(data[i].size),
                    data[i].field,
                    helpers.alignRight(
                        fonts[data[i].font],
                        Number(data[i].size),
                        data[i].field,
                        Number(data[i].position_x)
                    ),
                    Number(data[i].position_y),
                    false,
                );
            } else if (data[i].alignment === 'justify') {
                const length = Math.round(fonts[data[i].font]
                    .calculateTextDimensions(data[i].field, Number(data[i].size)).xMax);

                let yPos = Number(data[i].position_y);
                let result;

                if (length < 535) {
                    helpers.printText(
                        content,
                        fonts[data[i].font],
                        Number(data[i].size),
                        data[i].field,
                        Number(data[i].position_x),
                        Number(data[i].position_y),
                        true,
                    );
                } else {
                    result = data[i].field.replace(/.{130}\S*\s+/g, '$&@').split(/\s+@/);

                    for (let j = 0; j < result.length; j += 1) {
                        yPos -= 10;

                        if (j !== result.length - 1) {
                            helpers.printText(
                                content,
                                fonts[data[i].font],
                                Number(data[i].size),
                                result[j],
                                Number(data[i].position_x),
                                yPos,
                                true,
                            );
                        } else {
                            helpers.printText(
                                content,
                                fonts[data[i].font],
                                Number(data[i].size),
                                result[j],
                                Number(data[i].position_x),
                                yPos,
                                false,
                            );
                        }
                    }
                }
            } else if (data[i].position_x) {
                helpers.printText(
                    content,
                    fonts[data[i].font],
                    Number(data[i].size),
                    data[i].field,
                    Number(data[i].position_x),
                    Number(data[i].position_y),
                    false,
                );
            }
        } else if (data[i].type === 'image') {
            content.drawImage(
                Number(data[i].position_x),
                Number(data[i].position_y),
                logo,
                { transformation }
            );
        } else if (data[i].type === 'line') {
            content.drawPath(
                Number(data[i].position_x),
                Number(data[i].position_y),
                Number(data[i].position_x_end),
                Number(data[i].position_y_end),
            );
        }
    }

    pdfWriter
        .writePage(page)
        .end();
};

const concatPDFs = (filePath, files, pswd) => {
    const pdfWriter = hummus.createWriter(path.join(config.archive, filePath),
        {
            userPassword: pswd,
            ownerPassword: pswd,
            userProtectionFlag: 4,
        });
    for (let i = 0; i < files.length; i += 1) {
        pdfWriter.appendPDFPagesFromPDF(path.join(config.archive, files[i]));
    }
    pdfWriter.end();
};

export default { generatePDF, concatPDFs };
