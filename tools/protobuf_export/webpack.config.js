const path = require('path');

module.exports = {
    module: {
        rules: [{
            test: /\.proto$/,
            use: [{
                loader: 'raw-loader',
            }, ],
        }, ],
    },
    entry: './index.js',
    output: {
        path: path.resolve(__dirname, 'dist'),
        filename: 'pbwrapper.js',
        library: "pbwrapper",
    },
    mode: "production",
};

