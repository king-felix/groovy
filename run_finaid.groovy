// -- Run.groovy
// -- Date: Original 2014
// -- Author: Techproven, Inc
// -- 
// -- Description:  Transform files from Dynamic Forms format to Xtender/BDMS format plus copy them to 
//                  poller directory
// --  
// -- Change History
// -- 
// --	Modification History:
// --	Date       Who	  Why
// --	---------- ---	  ----------------------------------------------------------
// --   03/16/2015 R Farr * set SSN to Null for IndexImage Process.
// --   03/24/2015 R Farr * set rrrareq_info_access_ind =Y for insert
// --   12/17/2018 DB     * set new log output format

package com.techproven

import groovy.sql.Sql
import org.apache.log4j.*
import groovy.util.logging.*
import java.text.SimpleDateFormat
import javax.mail.*
import javax.mail.internet.*
import groovy.util.CliBuilder

@Log4j
class run {
	
	static main(args) {
		
		def cli = new CliBuilder(usage: 'run_finaid.groovy -m <mode>')
		cli.with {
			h longOpt: 'help', 'Show usage information'
			m longOpt: 'mode', args: 1, required: true, 'Mode.  Usually normal, but can be run under exceptions mode to analyze waiting files for errors and send e-mail notifications'
		}
		
		def options = cli.parse(args)
		def runtimeMode
		if (!options) {
			return
		} else {
			runtimeMode = options.m
		}
		
		def startTime = new Date()
		def startTimeFormatted = startTime
		startTimeFormatted = startTimeFormatted.format("MMddyy-hhmmssa")
		
		def config = new ConfigSlurper().parse(new File("config_finaid.groovy").toURL())
		
		this.initLogging(startTimeFormatted, config)
		def log = Logger.getLogger("run")
		log.level = Level."$config.options.logLevel"
		
		log.info "========> Starting Run <========"
		
		def resultMsg
		
		def cs = ConversionService.getInstance()
		def db = DatabaseService.getInstance()
		def email
		if (config.options.email.enabled) {
			email = EmailService.getInstance()	
			email.init()		
		}
		
		try {			
			db.init(config)
			cs.init(config, db, email)
			switch (runtimeMode){
				case "test":
					log.info "Running in test mode"
					cs.runTests()
					log.info "Tests completed"
					break
					
				case "exceptions":
					log.info "Running in exceptions mode"
					cs.processProfiles(runtimeMode)
					log.info "Exception checking completed.  Check e-mail for errors."
					break
					
				case "normal":				
					cs.processProfiles(runtimeMode)
					break
					
				default:
					log.error "Unknown mode specified.  Exiting..."					
					return				
			}
		} catch (ex) {
			log.error "Conversion encountered fatal error!"
			ex.printStackTrace()
			log.error ex
			cs.sendEmail([config.options.adminEmail], 'SCF Dyanmic Forms App Error', ex.toString())
		} finally {
			db.closeConnection()
			log.info "Processing completed!"
		}
	}
	
	static initLogging(def startTime, def config){
		def fileBase = "main-" + startTime
		def fileTime = new Date().format("yyyyMMdd")
		//def fileTime = new Date().format("yyyyMMddHHmmss")
		def logFile = "..\\logs\\DynamicFormsLoader_" + fileTime + ".log"
		
		def ma = new FileAppender()
		ma.setName("main")
		ma.setFile(logFile)
		ma.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"))
		ma.setThreshold(Level.DEBUG)
		ma.setAppend(true)
		ma.activateOptions()
		
		Logger.getRootLogger().addAppender(ma)
	}

}

class conversionException extends Exception{}

@Singleton
@Log4j
class ConversionService {

    def config
    def db
	def email
    def document
	def exceptions = []

    def init(def config, def db, def email) throws conversionException {
        this.config = config
        this.db = db
		this.email = email
        this.document = new Expando()
        log.level = Level."$config.options.logLevel"
    }

    def processProfiles(String runtimeMode) throws conversionException {		

        config.dynamicFormsProfiles.each { profile ->

            def profileName = profile.key
            def profileProperties
            if (config.dynamicFormsProfiles."$profileName".sameAs){
                profileProperties = config.dynamicFormsProfiles."$profileName".sameAs
            } else {
                profileProperties = profileName
            }
            def dir = new File("${config.options.dynamicFormsDownloads}\\${profileName}")

            dir.traverse() { file ->
				
				/* File Statuses
				 * 
				 * pending = the file was found and processing was attempted
				 * completed = the file was successfully processed
				 * failed = there was an issue.  The file was not processed.
				 * 
				 */
				
                def fileName = file.getName()
                document.fileName = fileName
				document.fileStatus = 'pending'
                log.info "Found: $file.path"
				
//				TODO: If we ever need to track the state of the document, we'll implement the database methods like the one referenced below
//				db.insertTrackingRecord(document)

				def exception = [:]
				
                def ext = ((file.getName().toString()).tokenize('.'))[-1]
                if (ext == 'pdf') {
					try {
						if (runtimeMode != 'exceptions'){
							document.type = 'pdf'
							def dest = new File("${config.options.xtenderFiles}\\$fileName")
							log.debug "========> Copying $file to $dest"
							copyFile(file, dest)
							document.fileStatus = 'completed'
							
							if (config.options.deleteDownloadedFilesOnCompletion){
								log.debug "Cleaning up document: $file"
								file.delete()
							}
						} else {
							log.debug "Running in exceptions mode, so skipping $file"
						}						
					} catch (ex) {
						document.fileStatus = 'failed'
						log.error "Failed to process PDF file"
						ex.printStackTrace()
						log.error ex
						sendEmail([config.options.adminEmail], "Failed to process PDF file", ex.message)						
					} finally {						
//						db.updateTrackingRecord(document)						
					}
                } else {
                    if (ext == 'txt') {
						try {
							def readyToWriteFile
							if (runtimeMode == 'exceptions'){
								log.debug "Running in exceptions mode, so $file won't be uploaded to Xtender."
								readyToWriteFile = false
							} else {								
								readyToWriteFile = true
							}
							
							document.type = 'txt'
							def lines = file.readLines()
							lines.each { line ->
								log.debug "========> Building Index for: $line"
								def fields = line.toString().tokenize(',')
	
								def xtenderApplication = config.dynamicFormsProfiles."$profileProperties".xtenderApplication
								document.destinationApplication = xtenderApplication
								def xtenderFields = config.xtenderApplications."$xtenderApplication".fields
								document.fieldCount = xtenderFields.size()
	
								// TODO: If this fails, we have more work to do.  We're short on time, so let's assume we'll need to do key ref checking for all form profiles.
								assert config.xtenderApplications."$xtenderApplication".keyRef == true

								def deliveredPath = fields[config.dynamicFormsProfiles."$profileProperties".filePath.toInteger() - 1].toString().toLowerCase()
								fields[config.dynamicFormsProfiles."$profileProperties".filePath.toInteger() - 1] = deliveredPath
								def dynamicPath = "${config.options.dynamicFormsDownloads.toString().toLowerCase()}\\${profileName.toString().toLowerCase()}"
								document.path = deliveredPath.replace(dynamicPath, config.options.xtenderFiles)
									
								def dfIdField = config.dynamicFormsProfiles."$profileProperties".fieldMapping.field1.toInteger()
								def id = fields[dfIdField-1].toUpperCase().trim()
                                                                //log.debug "ID field: $id"
								def rc = validateId(id)
								
								def personalInfo
								if (rc[0]) {
									personalInfo = db.getAllById(rc[1])
								} else {
									readyToWriteFile = false
									exception.put('file', file.path)
									exception.put('error', rc[1])
								}							
	
								if (personalInfo) {
									log.debug "Found this personal info: $personalInfo"
									document.field1 = "$personalInfo.ID"
									document.field2 = "$personalInfo.PIDM"
									document.field3 = config.dynamicFormsProfiles."$profileProperties".fieldMapping.field3
									document.field4 = "$personalInfo.FIRSTNAME"
									document.field5 = "$personalInfo.LASTNAME"
                                                                        //
                                                                        // set SSN to null for image index.
                                                                        document.field6 = ""
//									document.field6 = "$personalInfo.SSN"
									document.field7 = formatBirthDate("$personalInfo.BIRTHDATE")
									// log.debug "Field1: $document.field1"
									// log.debug "Field2: $document.field2"
									// log.debug "Field3: $document.field3"
									// log.debug "Field4: $document.field4"
									// log.debug "Field5: $document.field5"
									// log.debug "Field6: $document.field6"
									// log.debug "Field7: $document.field7"
									// log.debug "Field8: $document.field8"
									// log.debug "Field9: $document.field9"
									// log.debug "Field10: $document.field10"
									// log.debug "Field11: $document.field11"
									// log.debug "Field12: $document.field12"
									if (!document.field1 || !document.field2 || !document.field3 || !document.field4 || !document.field5){
										readyToWriteFile = false
										exception.put('file', file.path)
										exception.put('error', "Some fields are missing that should be present.  Processing this document would degrade the key reference system.")
									}

									config.dynamicFormsProfiles."$profileProperties".fieldMapping.each { key, value ->
										def fieldNumber = key - 'field'
										if (fieldNumber.toInteger() > 7) {
											if (value?.toString().isInteger()){												
												document."$key" = fields[value.toInteger() - 1]
											} else {
												document."$key" = value
											}
										}
									}	
									
									if (xtenderApplication == 'B-R-TREQ'){
										def rrrareqUpdateStatus = db.insertRrrareqRecordIfNeeded(document)
										if (rrrareqUpdateStatus[0] == 'inserted'){
											log.debug "RRRAREQ record insert or updated completed."
										} else {
											if (rrrareqUpdateStatus[0] == 'failed'){
												readyToWriteFile = false
												exception.put('file', file.path)
												exception.put('error', "Could not insert or updated RRRAREQ record: $rrrareqUpdateStatus[1]")
											}
										}
									}
								}
																							
								if (readyToWriteFile){
									printToFile(document)

									log.debug "Completed processing of $document"
									document.fileStatus = 'completed'
									
									if (config.options.deleteDownloadedFilesOnCompletion){
										log.debug "Cleaning up document: $file"
										file.delete()
									}
								} else {
									log.error "Can't process $file.  There was an exception: $exception.error"
									exceptions.add(exception)
								}																																					
							}
						} catch (ex) {
							document.fileStatus = 'failed'
							ex.printStackTrace()
							def msg = ["Failed to process dynamic forms TXT file: $file.path", "The error is: $ex.message"]
							log.error msg
							sendEmail([config.options.adminEmail, config.options.emailRecipients], msg[0], msg[1] )
						} finally {						
//							db.updateTrackingRecord(document)
						}
                    }
                }
            }
        }
		if (runtimeMode == 'exceptions' && exceptions.size() > 0){
			def mainMsg = """Some errors occured when attempting while processing files for loading into Xtender.  
These errors were caught during the validation phase before the actual upload was attempted.  
The files listed below were left in their original location under $config.options.dynamicFormsDownloads.  
Someone will need to manually correct these files before they can be uploaded.  This alert 
will continue to once per day these issues are resolved.\n\n"""
			def sb = new StringBuilder()
			sb.append(mainMsg)
			exceptions.each{ exception ->
				sb.append("- ${exception['file'].toString()}: ${exception['error'].toString()}\n")
			}
			sendEmail([config.options.adminEmail, config.options.emailRecipients], 
				'Dynamic Forms/Xtender Pre-Processing Exceptions', sb)				
		}
    }
	
	def validateId(def id) {
		
//		These examples tested as expected	
//		def good = 'G12345678'
//		def lower = 'g12345678'
//		def tooLong = 'G123456789'
//		def tooShort = 'G1234567'
//		def nonZero = 'GOO123456'
//		def invalidLetter = 'A12345678'
//		def invalidLetterInMiddle = 'G123456A7'		
		
		def validPattern = /G[0-9]{8}/
		def msg
		def rc = []
		
	    if (id ==~ validPattern) {	        
			log.debug "$id is good"
			rc = [true, id]
	    } else {
	        log.debug "$id is bad"
	        switch (id){
	            case { it.size() > 9 }:
	                msg = "$id is too long."
					log.error msg   
					rc = [false, msg]        
	                break	                
	            case { it.size() < 9 }:
	                msg = "$id is too short."
					log.error msg
					rc = [false, msg]
	                break	                
	            case ~/[a-z].*/:
	                log.error "$id has a lowercase letters"
	                id = id.toUpperCase()
					if (id ==~ validPattern){
						log.info "Fixed to $id"
						rc = [true, id]
					} else {
						msg = "$id is lower case.  Failed to fix $id by making upper case."
						log.error msg
						rc = [false, msg]
					}
	                break	                
	            case ~/.*[^G,0-9].*/:
	                log.error "$id has invalid characters.  Should only contain a G in the first position." 
					if (id ==~ /.*O.*/){
						log.warn "Looks like $id has letter O's instead of zeros.  Attempting to fix."
						id = id.replaceAll("O", "0")
						if (id ==~ validPattern){
							log.info "Successfully fixed id to $id"
							rc = [true, id]							
						} else {
							msg = "$id has invalid characters.  Failed to fix by changing O's to zeroes."
							log.error msg
							rc = [false, msg]
						}
					} else {
						msg = "$id has invalid characters."
						log.error msg
						rc = [false, msg]
					}
	                break           
	            
	            default:
	                msg = "$id is in an invalid and in an unrecognized format that can't be corrected."
					log.error msg
					rc = [false, msg]
	                break
		    }
			return rc
		}
	}

    def printToFile = { document ->
        def indexFile = new File("${config.options.xtenderIndexes}\\${document.fileName}")
        def sb = new StringBuilder()

        for (i in 0..document.fieldCount) {
            i++
            def field = "field$i"
            def val = document."$field"
            if(val == null){
                sb.append("|")
            } else {
                sb.append(val + "|")
            }
        }

        log.debug sb
        indexFile.append(sb + "\r\n")
        indexFile.append("@@${document.path}\r\n")
    }

    def copyFile = { File src, File dest->
        def input = src.newDataInputStream()
        def output = dest.newDataOutputStream()

        output << input

        input.close()
        output.close()
    }

    def formatBirthDate = { birthDate ->
        def defaultFormat = "yyyy-MM-dd HH:mm:ss.S"
        def inFormat = new SimpleDateFormat(defaultFormat)
        def inDate = inFormat.parse(birthDate)
        def outFormat = new SimpleDateFormat('dd-MMM-yyyy')
        def outDate = outFormat.format(inDate)
        outDate
    }

    def isInteger = { input ->
        try {
            Integer.parseInt(input);
        } catch(NumberFormatException e) {
            false
        }
        true
    }
	
	def sendEmail(List to, def subject, def body){
		if (email){
			log.info "Sending e-mail entitled: $subject to: $config.options.emailRecipients"
			email.sendMail(to, subject.toString(), body.toString())
		}		
	}
	
	def runTests(){
		testRrrareqInsert()
		testEmail()
	}
	
	def testRrrareqInsert(){
		def document = new Expando()
		document.field2 = '845089'
		document.field8 = '1415'
		document.field9 = 'STFDAP'
		
		def result = db.insertRrrareqRecordIfNeeded(document)
		
		assert result		
	}
	
	def testEmail(){
		throw new Exception("This is a test e-mail sent from the SCF dyanmic forms loader that simulates an error.")
	}
}

@Singleton
@Log4j
class DatabaseService {

    def url
    def driver
    def username
    def password
    def dbc
	def cs
	def config

    def init(def config) throws conversionException{
		log.level = Level."$config.options.logLevel"
        try {
            this.url = config.oracle.url
            this.driver = config.oracle.driver
            this.username = config.oracle.username
            this.password = config.oracle.password
			log.debug "Opening database connection"
            this.dbc = Sql.newInstance(url, username, password, driver)
			this.config = config
			this.cs = ConversionService.getInstance()
        } catch (ex) {
            log.error "Could not connect to database"
			ex.printStackTrace()
            log.error ex
        }
    }

    def closeConnection() {
        if (dbc){
            log.debug "Closing database connection"
            dbc.close()
        }
    }
	
	def getAllById(def id) throws conversionException {
		try {		
			def sql = """
				select s.spriden_id ID, s.spriden_pidm PIDM, s.spriden_first_name FIRSTNAME, s.spriden_last_name LASTNAME, p.spbpers_ssn SSN, p.spbpers_birth_date BIRTHDATE
				from spriden s, spbpers p
				where s.spriden_pidm = p.spbpers_pidm
				and s.spriden_change_ind is null
				and s.spriden_id = ${id}
				"""
//			println "sql is $sql"
			def row = dbc.firstRow(sql)
//			println "returned from spriden: $row"
			if (row){
				row
			} else {
				false
			}
		} catch (ex) {
			log.error "Failed to retrieve SPRIDEN record by ID for $id"
			ex.printStackTrace()
			log.error ex
		}
	}
	
	def checkForExistingTreqCode(def document) throws conversionException{
		try {
			def sql = """
				select * from rrrareq 
				where rrrareq_aidy_code = ${document.field8.toString()} 
				and rrrareq_pidm = ${document.field2.toString()}
				and rrrareq_treq_code = ${document.field9.toString()}
				"""
			def rows = dbc.rows(sql)
			if (rows){
				log.debug "pidm: $document.field2 has $rows.size requirements on file."
				return rows
			} else {
				return false
			}
		} catch (ex) {
			def msg = "TREQ code check failed for aid year: $document.field8, pidm: $document.field2, treq code: $document.field9"					
			ex.printStackTrace()
			log.error "$msg: $ex"
			cs.sendEmail([config.options.adminEmail], "Failed to check for RRRAREQ record", "$msg: $ex")
		}
	}
	
	def insertRrrareqRecordIfNeeded(def document) throws conversionException {
		try {
			def rc = []
			def msg
			def requirementAlreadyExists = checkForExistingTreqCode(document)
			if (requirementAlreadyExists){
				// do nothing
                // msg = "RRRAREQ record already exists"
				// log.debug msg
				// rc = ['exists', msg
				def sql = """
					update rrrareq  
					   set rrrareq_trst_code = 'P',
					       rrrareq_activity_date = SYSDATE,
					       rrrareq_stat_date = SYSDATE,
						   rrrareq_sys_ind = 'X'
					where rrrareq_aidy_code = ${document.field8.toString()} 
					and rrrareq_pidm = ${document.field2.toString()} 
					and rrrareq_treq_code = ${document.field9.toString()} 
		      	"""
				dbc.execute(sql)
                                //log.debug sql
				if (dbc.updateCount > 0){					
					msg = "Updated trst_code treq record in RRRAREQ to P for pidm: $document.field2, aid year: $document.field8, treq_code: $document.field9"
					log.info msg
					if (config.options.debugEmail.enabled){
						cs.sendEmail([config.options.adminEmail], "Update of RRRAREQ Just Happened", "Updated treq_code record in RRRAREQ for pidm: $document.field2, aid year: $document.field8, treq_code: $document.field9")	
					}
					rc = ['inserted', msg]
				} else {					
					msg = "Warning, tried to update for record in RRRAREQ for pidm: $document.field2, aid year: $document.field8, treq_code: $document.field9 but no rows were actually updateded."
					log.warn msg
					rc = ['failed', msg]
				}	
			} else {			
				def sql = """
					insert into rrrareq (rrrareq_aidy_code, rrrareq_pidm, rrrareq_treq_code, rrrareq_activity_date, 
						rrrareq_treq_desc, rrrareq_sat_ind, rrrareq_stat_date, rrrareq_est_date, rrrareq_trst_code, 
						rrrareq_pckg_ind, rrrareq_disb_ind, rrrareq_fund_code, rrrareq_sys_ind, rrrareq_sbgi_code, rrrareq_memo_ind,
						rrrareq_user_id, rrrareq_perk_mpn_exp_date, rrrareq_satisfied_date, rrrareq_mpn_first_disb_date, 
						rrrareq_mpn_signed_date, rrrareq_data_origin, rrrareq_trk_ltr_ind, rrrareq_info_access_ind, 
						rrrareq_period, rrrareq_sbgi_type_ind, rrrareq_term_code)
					values (${document.field8.toString()},${document.field2.toString()},${document.field9.toString()},SYSDATE,null,'N',SYSDATE,SYSDATE,'P','N','N',
						null,'X',null,'N','DYNAMICFORMS',null,null,null,null,null,null,'Y',null,null,null)
					"""
				dbc.execute(sql)
                                //log.debug sql
				if (dbc.updateCount > 0){					
					msg = "Inserted missing treq_code record into RRRAREQ for pidm: $document.field2, aid year: $document.field8, treq_code: $document.field9"
					log.info msg
					if (config.options.debugEmail.enabled){
						cs.sendEmail([config.options.adminEmail], "Insert into RRRAREQ Just Happened", "Inserted missing treq_code record into RRRAREQ for pidm: $document.field2, aid year: $document.field8, treq_code: $document.field9")	
					}
					rc = ['inserted', msg]
				} else {					
					msg = "Warning, tried to insert record into RRRAREQ for pidm: $document.field2, aid year: $document.field8, treq_code: $document.field9 but no rows were actually inserted."
					log.warn msg
					rc = ['failed', msg]
				}				
			}
			return rc
		} catch (ex) {
			def msg = "Failed to insert record for pidm: $document.field2 into RRRAREQ"
			ex.printStackTrace()
			log.error "$msg: $ex"
			cs.sendEmail([config.options.adminEmail], "Failed to insert RRRAREQ record", "$msg: $ex")
		}
	}
}

@Singleton
class EmailService {
 
	def username
	def password
	def host = "smtp.gmail.com"
 
	def init() {
		def config = new ConfigSlurper().parse(new File('config_finaid.groovy').toURL())
		username = config.options.email.username
		password = config.options.email.password
	}
	
	void sendMail(List to, String subject, String message){
		def props = new Properties()
		props.put "mail.smtps.auth", "true"
 
		def session = Session.getDefaultInstance props, null
 
		def msg = new MimeMessage(session)
 
		msg.setSubject subject
		msg.setText message
		
		if (to){
			to.each { list ->
				def recipients = list.tokenize(',')
				recipients.each{ recipient ->
					msg.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient.toString()))
				}
			}
			def transport = session.getTransport "smtps"
 
			try {
					transport.connect (host, username, password)
					transport.sendMessage (msg, msg.getAllRecipients())
//					println msg.getAllRecipients()
		    }
				catch (ex) {
					println ex.printStackTrace()
	        }
		}
	}
}

