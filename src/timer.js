class Timer {
    constructor(fn, interval) {
        this.fn = fn;
        this.interval = interval;
        this.timerObj = setInterval(fn, interval);
    }

    stop() {
        if (this.timerObj) {
            clearInterval(this.timerObj);
            this.timerObj = null;
        }
        return this;
    }

    // start timer using current settings (if it's not already running)
    start() {
        if (!this.timerObj) {
            this.stop();
            this.timerObj = setInterval(this.fn, this.interval);
        }
        return this;
    }
}

export default Timer;
