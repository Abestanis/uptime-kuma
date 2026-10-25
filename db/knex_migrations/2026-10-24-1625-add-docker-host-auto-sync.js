exports.up = function (knex) {
    return knex.schema.alterTable("docker_host", (table) => {
        table.boolean("auto_sync").notNullable().defaultTo(false);
    });
};

exports.down = function (knex) {
    return knex.schema.alterTable("docker_host", (table) => {
        table.dropColumn("auto_sync");
    });
};
