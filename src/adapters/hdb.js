/*
 * @moleculer/database
 * Copyright (c) 2022 MoleculerJS (https://github.com/moleculerjs/database)
 * MIT Licensed
 */

"use strict";

const _ = require("lodash");
const { flatten } = require("../utils");
const BaseAdapter = require("./base");

let hanaClientObj, connectionObj;

class HDBAdapter extends BaseAdapter {
	/**
	 * Constructor of adapter.
	 *
	 * @param  {Object?} opts
	 * @param  {String?} opts.dbName
	 * @param  {String?} opts.collection
	 * @param  {Object?} opts.mongoClientOptions More Info: https://mongodb.github.io/node-mongodb-native/4.1/interfaces/MongoClientOptions.html
	 * @param  {Object?} opts.dbOptions More Info: https://mongodb.github.io/node-mongodb-native/4.1/interfaces/DbOptions.html
	 */
	constructor(opts) {
		if (_.isString(opts)) opts = { uri: opts };

		super(opts);

		this.client = null;
		this.db = null;
	}

	/**
	 * The adapter has nested-field support.
	 */
	get hasNestedFieldSupport() {
		return false;
	}

	/**
	 * Initialize the adapter.
	 *
	 * @param {Service} service
	 */
	 init(service) {
		super.init(service);

		if (!this.opts.collection) {
			this.opts.collection = service.name;
		}

		try {
			this.hanaClientObj = require("@sap/hana-client");
		} catch (err) {
			/* istanbul ignore next */
			this.broker.fatal(
				"The 'ohana-node-orm' package is missing! Please install it with 'npm i ohana-node-orm --save' command.",
				err,
				true
			);
		}
	}

	/**
	 * Connect adapter to database
	 */
	async connect() {
		if(!this.connectionObj){
			try{
				let connParams = {
					serverNode: this.opts.host,
					uid: this.opts.user,
					pwd: this.opts.password
				}
				this.connectionObj = this.hanaClientObj.createConnection();
				this.connectionObj.connect(connParams);
			}catch(err){
				console.log(err);
			}
		}
	}

	/**
	 * Disconnect adapter from database
	 */
	async disconnect() {
		if(this.connectionObj){
			this.connectionObj.disconnect();
			this.connectionObj = undefined;
		}
	}

	/**
	 * Convert the param to ObjectId.
	 * @param {String|ObjectId} id
	 * @returns {ObjectId}
	 */
	stringToObjectID(id) {
		if (typeof id == "string" && ObjectId.isValid(id)) return ObjectId.createFromHexString(id);

		return id;
	}

	/**
	 * Convert ObjectID to hex string ID
	 * @param {ObjectId} id
	 * @returns {String}
	 */
	objectIDToString(id) {
		if (id && id.toHexString) return id.toHexString();
		return id;
	}

	/**
	 * Find all entities by filters.
	 *
	 * @param {Object} params
	 * @returns {Promise<Array>}
	 */
	find(params) {
		debugger;
		let schema = this.opts?.schema ? this.opts.schema : undefined;
		let table = this.opts?.collection ? this.opts.collection : undefined;
		let sqlQuery = 'select * from ' + schema + '.' + table; //Ex: 'select * from ADDTAX.TAXD0000'
		if(params?.query){
			let query = this.createQuery(params);
			if(query)
				sqlQuery = sqlQuery + ' ' + query;
		}
		
		let response = this.connectionObj.exec(sqlQuery);
		if(response)
			response.forEach((elem) => {
				elem._id = elem._ID;	
			});
		return response;
		// return this.createQuery(params).toArray();
	}

	/**
	 * Find an entity by query & sort
	 *
	 * @param {Object} params
	 * @returns {Promise<Object>}
	 */
	async findOne(params) {
		if (params.sort) {
			const res = await this.find(params);
			return res.length > 0 ? res[0] : null;
		} else {
			const q = { ...params.query };
			if (q._id) {
				q._id = this.stringToObjectID(q._id);
			}
			return this.collection.findOne(q);
		}
	}

	/**
	 * Find an entities by ID.
	 *
	 * @param {String|ObjectId} id
	 * @returns {Promise<Object>} Return with the found document.
	 *
	 */
	findById(id) {
		return this.collection.findOne({ _id: this.stringToObjectID(id) });
	}

	/**
	 * Find entities by IDs.
	 *
	 * @param {Array<String|ObjectId>} idList
	 * @returns {Promise<Array>} Return with the found documents in an Array.
	 *
	 */
	findByIds(idList) {
		return this.collection
			.find({
				_id: {
					$in: idList.map(id => this.stringToObjectID(id))
				}
			})
			.toArray();
	}

	/**
	 * Find all entities by filters and returns a Stream.
	 *
	 * @param {Object} params
	 * @returns {Promise<Stream>}
	 */
	findStream(params) {
		return this.createQuery(params).stream();
	}

	/**
	 * Get count of filtered entites.
	 *
	 * @param {Object} [params]
	 * @returns {Promise<Number>} Return with the count of documents.
	 *
	 */
	count(params) {
		debugger;
		let schema = this.opts?.schema ? this.opts.schema : undefined;
		let table = this.opts?.collection ? this.opts.collection : undefined;
		let sqlQuery = 'select count(*) from ' + schema + '.' + table; //Ex: 'select count(*) from ADDTAX.TAXD0000'
		let queryResult;
		try{
			queryResult = this.connectionObj.exec(sqlQuery); //Ex: [{COUNT(*): 1}]
		}catch(err){
			//TODO: Tratar erro de query
		}
		if(queryResult){
			try{
				return Object.values(queryResult[0])[0];
			}catch(err){
				//TODO: Tratar erro de resultado
			}
		}

		return 0; 

		//return this.connectionObj.exec('select count(*) from ADDTAX.TAXD0000');

	}

	/**
	 * Insert an entity.
	 *
	 * @param {Object} entity
	 * @returns {Promise<Object>} Return with the inserted document.
	 *
	 */
	async insert(entity) {
		debugger;
		let schema = this.opts?.schema ? this.opts.schema : undefined;
		let table = this.opts?.collection ? this.opts.collection : undefined;
		let queryFields = '';
		let queryValues = '';

		this.service.$fields.forEach((field) => {
			//if(entity[field.columnName] && field.columnName != "_id"){
			if(entity[field.columnName]){
				queryFields = '' + queryFields + field.columnName + ', ';
				if(field.columnType == 'string')
					queryValues = '' + queryValues + "'" + entity[field.columnName] + "', ";
				else
					queryValues = '' + queryValues + entity[field.columnName] + ', ';
			}
		
		});

		queryFields = queryFields.substring(0, queryFields.length - 2);
		queryValues = queryValues.substring(0, queryValues.length - 2);

		//let sqlQuery = 'insert into ' + schema + '.' + table + '(' + Object.keys(entity) + ') VALUES(' + Object.values(entity) + ')'; //Ex: 'INSERT INTO ADDTAX.TAXD0000(id, indoc) VALUES(12321231, 1)'
		let sqlQuery = 'insert into ' + schema + '.' + table + '(' + queryFields + ') VALUES(' + queryValues + ')'; //Ex: 'INSERT INTO ADDTAX.TAXD0000(id, indoc) VALUES(12321231, 1)'
		let insertResult;
		try{
			insertResult = this.connectionObj.exec(sqlQuery); //Ex: [{COUNT(*): 1}]
		}catch(err){
			//TODO: Tratar erro de query
		}
		if(insertResult){
			return entity;
		}
		return entity;
	}

	/**
	 * Insert many entities
	 *
	 * @param {Array<Object>} entities
	 * @param {Object?} opts
	 * @param {Boolean?} opts.returnEntities
	 * @returns {Promise<Array<Object|any>>} Return with the inserted IDs or entities.
	 *
	 */
	async insertMany(entities, opts = {}) {
		for (const entity of entities) {
			if (entity._id) {
				entity._id = this.stringToObjectID(entity._id);
			}
		}
		const res = await this.collection.insertMany(entities);
		if (!res.acknowledged) throw new Error("MongoDB insertMany failed.");
		return opts.returnEntities ? entities : Object.values(res.insertedIds);
	}

	/**
	 * Update an entity by ID
	 *
	 * @param {String} id
	 * @param {Object} changes
	 * @param {Object} opts
	 * @returns {Promise<Object>} Return with the updated document.
	 *
	 */
	async updateById(id, changes, opts) {
		if(id){
			let schema = this.opts?.schema ? this.opts.schema : undefined;
			let table = this.opts?.collection ? this.opts.collection : undefined;
			let setString = '';
			Object.entries(changes).forEach(([key, value]) => {
				let keyDef = this.service.$fields.find((elem) => elem?.columnName == key);
				let keyIsString = false;
				if(keyDef && keyDef?.columnType == 'string')
					keyIsString = true;
				if(keyIsString)
					setString = setString + ' ' + key + "='" + value + "',"
				else
				setString = setString + ' ' + key + "=" + value + ","
			});
			if(setString){
				setString = setString.substring(0, setString.length - 1);
				let sqlQuery = "UPDATE " + schema + '.' + table + " SET " + setString + "WHERE _ID = '" + id + "'"; //Ex.: UPDATE ADDTAX.TAXD0000 SET DOMIN = 'SP 3550308', PROCX='03500' WHERE _ID = '26b812d0-51a7-11ee-94c4-e7045b3b40cd' 				
				let insertResult;
				try{
					insertResult = this.connectionObj.exec(sqlQuery); //Ex: [{COUNT(*): 1}]
				}catch(err){
					//TODO: Tratar erro de query
				}
				if(insertResult){
					let findResult = await this.find({query: { id:id}});
					if(findResult)
						return findResult[0];
				}
			};
		}
		// const raw = opts && opts.raw ? true : false;
		// if (!raw) {
		// 	// Flatten the changes to dot notation
		// 	changes = flatten(changes, { safe: true });
		// }

		// const res = await this.collection.findOneAndUpdate(
		// 	{ _ID: this.stringToObjectID(id) },
		// 	raw ? changes : { $set: changes },
		// 	{ returnDocument: "after" }
		// );
		// return res.value;
		return;
	}

	/**
	 * Update many entities
	 *
	 * @param {Object} query
	 * @param {Object} changes
	 * @param {Object} opts
	 * @returns {Promise<Number>} Return with the count of modified documents.
	 *
	 */
	async updateMany(query, changes, opts) {
		const raw = opts && opts.raw ? true : false;
		if (!raw) {
			// Flatten the changes to dot notation
			changes = flatten(changes, { safe: true });
		}

		const res = await this.collection.updateMany(query, raw ? changes : { $set: changes });
		return res.modifiedCount;
	}

	/**
	 * Replace an entity by ID
	 *
	 * @param {String} id
	 * @param {Object} entity
	 * @returns {Promise<Object>} Return with the updated document.
	 *
	 */
	async replaceById(id, entity) {
		const res = await this.collection.findOneAndReplace(
			{ _id: this.stringToObjectID(id) },
			entity,
			{ returnDocument: "after" }
		);
		return res.value;
	}

	/**
	 * Remove an entity by ID
	 *
	 * @param {String} id
	 * @returns {Promise<any>} Return with ID of the deleted document.
	 *
	 */
	async removeById(id) {
		await this.collection.findOneAndDelete({ _id: this.stringToObjectID(id) });
		return id;
	}

	/**
	 * Remove entities which are matched by `query`
	 *
	 * @param {Object} query
	 * @returns {Promise<Number>} Return with the number of deleted documents.
	 *
	 */
	async removeMany(query) {
		const res = await this.collection.deleteMany(query);
		return res.deletedCount;
	}

	/**
	 * Clear all entities from collection
	 *
	 * @returns {Promise<Number>}
	 *
	 */
	async clear() {
		const res = await this.collection.deleteMany({});
		return res.deletedCount;
	}

	/**
	 * Convert DB entity to JSON object.
	 *
	 * @param {Object} entity
	 * @returns {Object}
	 */
	entityToJSON(entity) {
		let json = Object.assign({}, entity);
		if (this.opts.stringID !== false && entity._id)
			json._id = this.objectIDToString(entity._id);
		return json;
	}

	/**
	 * Check the IDs in the `query` and convert to ObjectID.
	 * @param {Object} q
	 * @returns {Object}
	 */
	convertIDToObjectID(query) {
		if (query && query._id) {
			const q = { ...query };
			if (typeof q._id == "object" && Array.isArray(q._id.$in)) {
				q._id.$in = q._id.$in.map(this.stringToObjectID);
			} else {
				q._id = this.stringToObjectID(q._id);
			}
			return q;
		}
		return query;
	}

	// /**
	//  * Create a query based on filters
	//  *
	//  * Available filters:
	//  *  - search
	//  *  - searchFields
	//  * 	- sort
	//  * 	- limit
	//  * 	- offset
	//  *  - query
	//  *
	//  * @param {Object} params
	//  * @param {Object?} opts
	//  * @param {Boolean?} opts.counting
	//  * @returns {Query}
	//  * @memberof MemoryDbAdapter
	//  */
	// createQuery(params, opts = {}) {
	// 	const fn = opts.counting ? this.collection.countDocuments : this.collection.find;
	// 	let q;
	// 	if (params) {
	// 		if (_.isString(params.search) && params.search !== "") {
	// 			// Full-text search
	// 			// More info: https://docs.mongodb.com/manual/reference/operator/query/text/
	// 			q = fn.call(
	// 				this.collection,
	// 				Object.assign({}, params.query || {}, {
	// 					$text: {
	// 						$search: params.search
	// 					}
	// 				})
	// 			);

	// 			if (!opts.counting) {
	// 				if (q.project) q.project({ _score: { $meta: "textScore" } });

	// 				if (q.sort) {
	// 					if (params.sort) {
	// 						const sort = this.transformSort(params.sort);
	// 						if (sort) q.sort(sort);
	// 					} else {
	// 						q.sort({
	// 							_score: {
	// 								$meta: "textScore"
	// 							}
	// 						});
	// 					}
	// 				}
	// 			}
	// 		} else {
	// 			const query = this.convertIDToObjectID(params.query);

	// 			q = fn.call(this.collection, query);

	// 			// Sort
	// 			if (!opts.counting && params.sort && q.sort) {
	// 				const sort = this.transformSort(params.sort);
	// 				if (sort) q.sort(sort);

	// 				// Collation
	// 				// https://docs.mongodb.com/manual/reference/method/cursor.collation/
	// 				if (params.collation) q.collation(params.collation);
	// 			}
	// 		}

	// 		if (!opts.counting) {
	// 			// Offset
	// 			if (_.isNumber(params.offset) && params.offset > 0) q.skip(params.offset);

	// 			// Limit
	// 			if (_.isNumber(params.limit) && params.limit > 0) q.limit(params.limit);
	// 		}

	// 		// Hint
	// 		// https://docs.mongodb.com/manual/reference/method/cursor.hint/
	// 		if (params.hint) q.hint(params.hint);

	// 		return q;
	// 	}

	// 	// If not params
	// 	return fn.call(this.collection, {});
	// }

	/**
	 * Convert the `sort` param to a `sort` object to Mongo queries.
	 *
	 * @param {String|Array<String>|Object} paramSort
	 * @returns {Object} Return with a sort object like `{ "votes": 1, "title": -1 }`
	 * @memberof MongoDbAdapter
	 */
	transformSort(sort) {
		if (typeof sort == "string") sort = [sort];
		if (Array.isArray(sort)) {
			return sort.reduce((res, s) => {
				if (s.startsWith("-")) res[s.slice(1)] = -1;
				else res[s] = 1;
				return res;
			}, {});
		}

		return sort;
	}

	/**
	 * Create an index.
	 *
	 * @param {Object} def
	 * @param {String|Array<String>|Object} def.fields
	 * @param {String?} def.name
	 * @param {Boolean?} def.unique
	 * @param {Boolean?} def.sparse
	 * @param {Number?} def.expireAfterSeconds
	 */
	createIndex(def) {
		let fields;
		if (typeof def.fields == "string") fields = { [def.fields]: 1 };
		else if (Array.isArray(def.fields)) {
			fields = def.fields.reduce((a, b) => {
				a[b] = 1;
				return a;
			}, {});
		} else {
			fields = def.fields;
		}
		return this.collection.createIndex(fields, def);
	}

	/**
	 * Remove an index by name or fields.
	 *
	 * @param {Object} def
	 * @param {String|Array<String>|Object} def.fields
	 * @param {String?} def.name
	 * @returns {Promise<void>}
	 */
	removeIndex(def) {
		if (def.name) return this.collection.dropIndex(def.name);
		else return this.collection.dropIndex(def.fields);
	}

	/**
	 * Create a query based on filters
	 *
	 * Available filters:
	 *  - search
	 *  - searchFields
	 * 	- sort
	 * 	- limit
	 * 	- offset
	 *  - query
	 *
	 * @param {Object} params
	 * @param {Object?} opts
	 * @param {Boolean?} opts.counting
	 * @returns {Query}
	 * @memberof MemoryDbAdapter
	 */
	createQuery(params, opts = {}) {
		//let q = this.getTable();
		let q = 'WHERE ';
		//if (opts.counting) q = q.count({ count: "*" });
		if (params) {
			const query = params.query ? Object.assign({}, params.query) : {};

			Object.entries(query).forEach(([key, value]) => {
				let keyDef = this.service.$fields.find((elem) => elem?.columnName == key);
				let keyIsString = false;
				if(key == 'id' || key == '_id'){
					key = '_ID';
					keyIsString = true;
				}
				if(keyDef && keyDef?.columnType == 'string')
					keyIsString = true;
				if (typeof value == "object" && value != null) {
					if (value.$in && Array.isArray(value.$in)) {
						//q = q.whereIn(key, value.$in);
						q = q + 'IMPLEMENTAR';
					} else if (value.$nin && Array.isArray(value.$nin)) {
						//q = q.whereNotIn(key, value.$nin);
						q = q + 'IMPLEMENTAR';
					} else if (value.$gt) {
						//q = q.where(key, ">", value.$gt);
						if(keyIsString)
							q = q + key + " > '" + value.$gt + "' AND ";
						else
							q = q + key + ' > ' + value.$gt + ' AND ';
					} else if (value.$gte) {
						//q = q.where(key, ">=", value.$gte);
						if(keyIsString)
							q = q + key + " >= '" + value.$gte + "' AND ";
						else
							q = q + key + ' >= ' + value.$gte + ' AND ';
					} else if (value.$lt) {
						//q = q.where(key, "<", value.$lt);
						if(keyIsString)
							q = q + key + " < '" + value.$lt + "' AND ";
						else
							q = q + key + ' < ' + value.$lt + ' AND ';
					} else if (value.$lte) {
						//q = q.where(key, "<=", value.$lte);
						if(keyIsString)
							q = q + key + " <= '" + value.$lte + "' AND ";
						else
							q = q + key + ' <= ' + value.$lte + ' AND ';
					} else if (value.$eq) {
						//q = q.where(key, "=", value.$eq);
						if(keyIsString)
							q = q + key + " = '" + value.$eq + "' AND ";
						else
							q = q + key + ' = ' + value.$eq + ' AND ';
					} else if (value.$ne) {
						//q = q.where(key, "=", value.$ne);
						if(keyIsString)
							q = q + key + " != '" + value.$ne + "' AND ";
						else
							q = q + key + ' != ' + value.$ne + ' AND ';
					} else if (value.$exists === true) {
						//q = q.whereNotNull(key);
						q = q + 'IMPLEMENTAR';
					} else if (value.$exists === false) {
						//q = q.whereNull(key);
						q = q + 'IMPLEMENTAR';
					} else if (value.$raw) {
						if (typeof value.$raw == "string") {
							//q = q.whereRaw(value.$raw);
							q = q + 'IMPLEMENTAR';
						} else if (typeof value.$raw == "object") {
							//q = q.whereRaw(value.$raw.condition, value.$raw.bindings);
							q = q + 'IMPLEMENTAR';
						}
					}
				} else {
					//q = q.where(key, value);
					if(keyIsString)
						q = q + key + " = '" + value + "' AND ";
					else
						q = q + key + ' = ' + value + ' AND ';
				}
			});

			//TODO: IMPLEMENTAR FUNCIONALIDADES ABAIXO
			// Text search
			// if (_.isString(params.search) && params.search !== "" && params.searchFields) {
			// 	params.searchFields.forEach((field, i) => {
			// 		const fn = i == 0 ? "where" : "orWhere";
			// 		q = q[fn](field, "like", `%${params.search}%`);
			// 	});
			// }

			// // Sort
			// if (!opts.counting && params.sort) {
			// 	let pSort = params.sort;
			// 	if (typeof pSort == "string") pSort = [pSort];
			// 	pSort.forEach(field => {
			// 		if (field.startsWith("-")) q = q.orderBy(field.slice(1), "desc");
			// 		else q = q.orderBy(field, "asc");
			// 	});
			// }

			// // Limit
			// if (!opts.counting && _.isNumber(params.limit) && params.limit > 0)
			// 	q.limit(params.limit);

			// // Offset
			// if (!opts.counting && _.isNumber(params.offset) && params.offset > 0) {
			// 	if (!params.sort && this.opts.knex.client == "mssql") {
			// 		// MSSQL can't use offset without sort.
			// 		// https://github.com/knex/knex/issues/1527
			// 		q = q.orderBy(this.idFieldName, "asc");
			// 	}
			// 	q.offset(params.offset);
			// }
		}

		// If not params
		if(q != 'WHERE '){
			q = q.substring(0, q.length - 5); //Remove o ' AND ' do final da string
			return q;
		}
		else
			return '';
	}
}

module.exports = HDBAdapter;