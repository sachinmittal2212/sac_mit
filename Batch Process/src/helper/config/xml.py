'''
	Purpose of this file to gather and validate config file.
'''
from lxml import etree
import xmltodict as convt
from helper.log.logging import logger 


class XMLConfig:
	def __init__(self, xml_data=None, xsd_data=None, xml_path=None, xsd_path=None):
		self.xsd=xsd_data
		self.xml=xml_data
		self.xml_path=xml_path
		self.xsd_path=xsd_path
		if xsd_data!=None or xsd_path!=None:
			if xml_path!=None and xsd_path!=None:
				logger.info('Validating XML. XSD: {}'.format(xsd_path))
				if(not(self.validate_xml_path())):
					raise Exception('Invalid XML. Please check XML: {} or XSD: {}'
					.format(xml_path, xsd_path))
			if self.xml!=None and self.xsd!=None:
				if(not(self.validate_xml_binary())):
					raise Exception('Invalid XML {}, Validator: {}'.format(xml_path, xsd_path))
		else:
			logger.warning('Unable to find XSD details to validate XML. ')

	def validate_xml_path(self):
		validate = True
		from lxml.etree import XMLSchema, XML
		XMLschema = XMLSchema(file=self.xsd_path)
		xml_file = open(self.xml_path, mode='rb')
		xml = XML(xml_file.read())
		if not XMLschema.validate(xml):
			for error in XMLschema.error_log:
				logger.info(error.message, error.line, error.column)
				validate = False
		return validate

	def validate_xml_binary(self):
		validate_xml=etree.XML(self.xsd)
		validate_schema=etree.XMLSchema(validate_xml)
		config_tree=etree.fromstring(self.xml)
		valid=validate_schema.validate(config_tree)
		return valid

	def xml_to_dict(self):
		if self.xml is not None:
			dictJson=convt.parse(self.xml)
		elif self.xml_path is not None:
			cfg_file = open(self.xml_path, 'rb').read()
			dictJson=convt.parse(cfg_file)
		return dictJson
