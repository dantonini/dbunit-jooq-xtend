package org.example

import java.io.StringReader
import java.util.Map
import org.dbunit.PropertiesBasedJdbcDatabaseTester
import org.dbunit.dataset.IDataSet
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder
import org.dbunit.operation.DatabaseOperation
import org.example.dbunit.jooq.xtend.tables.Table1
import org.jooq.Field
import org.jooq.impl.DSL
import org.jooq.impl.TableImpl
import org.junit.BeforeClass
import org.junit.Test

import static org.hamcrest.Matchers.*
import static org.junit.Assert.*
import org.dbunit.database.IDatabaseConnection
import com.google.common.io.Files
import com.google.common.base.Charsets
import java.io.File
import com.google.common.io.CharStreams
import java.io.InputStreamReader
import java.io.InputStream

class TypeSafeDataset {

	static PropertiesBasedJdbcDatabaseTester dbTester

	@BeforeClass static def void beforeClass() {
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_DRIVER_CLASS, "org.h2.Driver");
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_CONNECTION_URL,
			"jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_USERNAME, "sa");
		System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_PASSWORD, "");
		
		dbTester = new PropertiesBasedJdbcDatabaseTester()
		
		TypeSafeDataset.getResourceAsStream("/database.ddl").executeStatements
	}
	
	@Test def void test() {
		dataset(
			table1[#{ID >> 12, COLUMN_A >> "2", COLUMN_B >> 12}], // type safe operator >>
			table1[#{ID -> "12", COLUMN_A -> 2, COLUMN_B -> "12"}] // not type safe operator ->
		).CLEAN_INSERT

		assertThat(JOOQ.select(DSL.count()).from(Table1.TABLE1).fetchOne.value1, is(2))
	}

	def table1((Table1)=>Map fields) {
		val map = fields.apply(Table1.TABLE1).mapKeys [
			(it as Field).name
		]
		return Table1.TABLE1.toFlatXml(map)
	}

	/**
	 * ovverride operator >> to force typesafety
	 */
	def static <T> Pair<Field<T>, T> operator_doubleGreaterThan(Field<T> field, T value) {
		return Pair.of(field, value);
	}

	private def void CLEAN_INSERT(IDataSet dataset) {
		var IDatabaseConnection dbuConnection = null
		try {
			dbuConnection = dbTester.connection
			DatabaseOperation.CLEAN_INSERT.execute(dbuConnection, dataset)
		} finally {
			dbuConnection.close();
		}
	}

	private def JOOQ() {
		DSL.using(dbTester.connection.connection)
	}

	private def dataset(CharSequence ... xmlSnippet) {
		return '''
			<dataset>
			«FOR s : xmlSnippet»
				«s»
			«ENDFOR»
			</dataset>
		'''.asIDataSet
	}

	private def IDataSet asIDataSet(CharSequence sequence) {
		return new FlatXmlDataSetBuilder().build(new StringReader(sequence.toString))
	}

	private def toFlatXml(TableImpl table, Map<String, Object> map) {
		return '''<«table.name» «map.entrySet.map[key+'="'+value+'"'].join(' ')» />'''
	}

	private def <K, V, T> Map<T, V> mapKeys(Map<K, V> fields, (K)=>T function) {
		val tmp = newHashMap
		for (es : fields.entrySet) {
			tmp.put(function.apply(es.key), es.value)
		}
		return tmp
	}
	
	private static def executeStatements(InputStream is) {
		val c = dbTester.connection.connection
		c.createStatement().execute(CharStreams.toString(new InputStreamReader(is, Charsets.UTF_8)))
		c.commit
		c.close
	}
}
