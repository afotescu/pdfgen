const alignRight = (fontObject, fontSize, textContent, x) => {
    return (x - Math.round(fontObject.calculateTextDimensions(textContent, fontSize).xMax));
};

const getOptions = (size, font, color) => {
    return {
        size: parseFloat(size),
        font,
        color: 0x00,
    };
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

export default { alignRight, getOptions, leftpad };
