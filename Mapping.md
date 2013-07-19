# Triple Mapping

Default indices of NativeStore: spoc, psoc


## Settings

	triplestore [hash]
		prefix -) (prefix)
		mapping -) (mapping scheme)
		# additional meta data
		[meta:xxx -) (xxx)]


## Complete variant: hash per quadruple, (z)set for indices

#### Discussion

pro

* fine granularity
* optimal support for contexts

cons

* lot of data

#### Usage

* use intersection to find matching results


#### Data Mapping

	SINTER subject:(subject)

	subject:(subject) [set/zset]
		(tripleid-1)
		(tripleid-2)
		...
		(tripleid-n)
		
	predicate:(predicate) [set/zset]
		(tripleid-1)
		(tripleid-2)
		...
		(tripleid-n)
	
	object:(object) [set/zset]
		(tripleid-1)
		(tripleid-2)
		...
		(tripleid-n)
	
	context:(ctx) [set/zset]
		(tripleid-1)
		(tripleid-2)
		...
		(tripleid-n)
	
	triple:(tripleid) [hash]
		subject: (subj)
		predicate: (pred)
		object: (val)
		context: (ctx)


## Simple Variant: set/zset per index

### spoc
	spoc [set/zset]
		(subj)|(pred)|(obj)|(ctx)

### psoc
	psoc [set/zset]
		(pred)|(subj)|(obj)|(ctx)
	

## Variant: hash per first two index fields

### spoc
	sp:(subj)|(pred) [hash]
		object: (obj)
		context: (ctx)

### psoc
	ps:(pred)|(subj) [hash]
		object: (obj)
		context: (ctx)


## Variant: set/zset per first two index fields

### spoc
	sp:(subj)|(pred) [set/zset]
		(obj)|(ctx)
	

### psoc
	ps:(pred)|(subj) [set/zset]
		(obj)|(ctx)


