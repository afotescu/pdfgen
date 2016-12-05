const alignRight = (fontObject, fontSize, textContent, x) => {
    return (x - Math.round(fontObject.calculateTextDimensions(textContent, fontSize).xMax));
};

const alignCenter = (fontObject, fontSize, textContent, x) => {
    return (x - (Math.round(fontObject.calculateTextDimensions(textContent, fontSize).xMax / 2)));
};

const printText = (pdfWriter, fontObject, fontSize, textContent, x, y, justify) => {
    let space;

    if(justify){
        space = Number((535 - Math.round(fontObject.calculateTextDimensions(textContent,
                fontSize).xMax)) / textContent.match(/ /g).length)
    } else {
        space = 0;
    }

    pdfWriter
        .BT()
        .Tw(space)
        .Tf(fontObject,fontSize)
        .Td(x,y)
        .Tj(textContent)
        .ET();
};

const leftpad = (str, len, ch) => {
    let rCh = ch;
    let rStr = String(str);
    let i = -1;
    if (!ch && ch !== 0) {
        rCh = ' ';
    }
    const rLen = len - str.length;
    while (rLen > i) {
        rStr = rCh + rStr;
        i += 1;
    }
    return rStr;
};

export default { alignRight, alignCenter, printText, leftpad };
