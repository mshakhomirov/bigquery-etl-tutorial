"use strict";

let lambda = require('./index');
let event = require('./event.json');

lambda.handler(event,
    {
        done    : r  => console.log("done, result returned:\n", r),
        succeed : r  => console.log("success, result returned:\n", r),
        fail    : e  => console.log(e)
    }
);
