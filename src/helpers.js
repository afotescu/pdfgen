const alignRight = (fontObject, fontSize, textContent, x) => {
    return (x - Math.round(fontObject.calculateTextDimensions(textContent, fontSize).xMax));
};

const getOptions = (size, font, color) => {
    return {
        size: parseFloat(size),
        font,
        color: 0x00,
    }
};

export default {alignRight, getOptions};
