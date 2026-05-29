"use strict";
const fs = require("fs");
const path = require("path");
const assert = require("assert");

// Read the HTML template once for all assertions.
const html = fs.readFileSync(path.join(__dirname, "../redis.html"), "utf8");

// Narrow to the RED.library.create() call inside the redis-lua-script registerType block.
function extractLibraryCreateBody(source) {
    const m = source.match(/registerType\('redis-lua-script'[\s\S]*?RED\.library\.create\(\{([\s\S]*?)\}\s*\)/);
    return m ? m[1] : null;
}

// Extract a simple scalar option (string value) from the library.create() body.
function extractLibraryOption(source, key) {
    const body = extractLibraryCreateBody(source);
    if (!body) return null;
    const m = body.match(new RegExp(key + "\\s*:\\s*[\"']([^\"']+)[\"']"));
    return m ? m[1] : null;
}

// Return the raw fields array source text from the library.create() body.
function extractFieldsBody(source) {
    const body = extractLibraryCreateBody(source);
    if (!body) return null;
    const m = body.match(/fields\s*:\s*\[([\s\S]*?)\]/);
    return m ? m[1] : null;
}

// Returns true if the fields body declares the given field as a plain string entry.
function hasStringField(fieldsBody, name) {
    return new RegExp(`['"]${name}['"]`).test(fieldsBody);
}

// Returns true if the fields body declares the given field as an object with get/set.
function hasObjectField(fieldsBody, name) {
    // Look for   name: 'foo',  followed (anywhere in the object) by get: and set:
    const objectPattern = new RegExp(
        `name\\s*:\\s*['"]${name}['"][\\s\\S]*?get\\s*:[\\s\\S]*?set\\s*:`
    );
    return objectPattern.test(fieldsBody);
}

describe("redis-lua-script UI template", function () {

    describe("RED.library.create type", function () {
        it("does not contain a period", function () {
            const type = extractLibraryOption(html, "type");
            assert.ok(type !== null, "RED.library.create() type should be present in the template");
            assert.ok(
                !type.includes("."),
                `library type "${type}" must not contain a period — ` +
                `RED.menu.init uses jQuery's #id selector which treats '.' as a class ` +
                `separator, so "node-input-${type}-lookup" would never be found and ` +
                `the Open/Save Library menu would not attach to the button`
            );
        });

        it("produces a valid DOM id for the lookup button", function () {
            const type = extractLibraryOption(html, "type");
            assert.ok(type !== null, "RED.library.create() type should be present in the template");
            const buttonId = `node-input-${type}-lookup`;
            // A valid id for jQuery's #id selector must not contain: . # [ ] ( ) etc.
            assert.ok(
                /^[A-Za-z0-9_-]+$/.test(buttonId),
                `Generated button id "${buttonId}" must contain only alphanumerics, hyphens, and underscores`
            );
        });

        it("type is 'lua'", function () {
            const type = extractLibraryOption(html, "type");
            assert.strictEqual(type, "lua",
                "library type should be 'lua' (dot-free) so the lookup button id " +
                "node-input-lua-lookup is a valid jQuery selector"
            );
        });
    });

    describe("RED.library.create ext", function () {
        it("save dialog default filename uses .lua extension", function () {
            const ext = extractLibraryOption(html, "ext");
            assert.strictEqual(ext, "lua",
                "ext should be 'lua' so the Save to Library dialog pre-fills the filename as <name>.lua"
            );
        });
    });

    describe("RED.library.create fields", function () {
        let fieldsBody;
        before(function () {
            fieldsBody = extractFieldsBody(html);
            assert.ok(fieldsBody !== null, "fields array should be present in RED.library.create()");
        });

        it("includes 'name' and 'keyval' as plain string fields", function () {
            assert.ok(hasStringField(fieldsBody, "name"),   "fields should include 'name'");
            assert.ok(hasStringField(fieldsBody, "keyval"), "fields should include 'keyval'");
        });

        it("declares 'stored' as an object field with get and set", function () {
            assert.ok(
                hasObjectField(fieldsBody, "stored"),
                "'stored' must be an object field with get/set — plain .val() does not read " +
                "or write checkbox checked state"
            );
        });

        it("declares 'block' as an object field with get and set", function () {
            assert.ok(
                hasObjectField(fieldsBody, "block"),
                "'block' must be an object field with get/set — it was previously missing " +
                "from fields entirely, so Block Commands was never saved to or loaded from the library"
            );
        });

        it("checkbox get() returns a string, not a boolean", function () {
            // Node-RED's saveLibraryEntry passes every metadata value through
            // toSingleLine(text) which calls text.replace(...). If get() returns
            // a boolean, .replace is not a function and the save throws.
            assert.ok(
                /["'"]true["'"]/.test(fieldsBody) && /["'"]false["'"]/.test(fieldsBody),
                "checkbox get() must return the string \"true\" or \"false\", not a boolean, " +
                "because Node-RED calls text.replace() on every metadata value when writing the file"
            );
        });
    });
});
