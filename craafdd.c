/* Critical Resources Auditing and context-Aware Fencing Decisions Daemon (CRAAFDD) v1.0
 **********************************************************************
 *	 ██████╗██████╗  █████╗  █████╗ ███████╗██████╗ ██████╗       *
 *      ██╔════╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗██╔══██╗      *
 *	██║     ██████╔╝███████║███████║█████╗  ██║  ██║██║  ██║      *
 *	██║     ██╔══██╗██╔══██║██╔══██║██╔══╝  ██║  ██║██║  ██║      *
 *	╚██████╗██║  ██║██║  ██║██║  ██║██║     ██████╔╝██████╔╝      *
 * 	 ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═════╝ ╚═════╝       *
 **********************************************************************
 * License: AGPL v3.
 * Giuseppe Calò                giuseppe.calo@cmcc.it
 * Danilo Mazzarella		danilo.mazzarella@cmcc.it
 * Marco Chiarelli              marco.chiarelli@cmcc.it
 *                              marco_chiarelli@yahoo.it
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <signal.h>
#include <time.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <netinet/in.h>
#include <net/if.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <errno.h>

#define CRAAFDD_VERSION "v1.0"
#define CRAAFDD_DESCRIPTION "Critical Resources Auditing and context-Aware Fencing Decisions Daemon (CRAAFDD) "CRAAFDD_VERSION

#define MAX_BUFTIME_LENGTH 160
// decomment the line below in order to enter debug mode
// #define DEBUG_MODE

#define DEFAULT_AMIQUORUM 1
#define DEFAULT_MIN_QUORUM_NODES -1 // dynamic

#define SAMPLING_PERIOD 2 // 3
#define SAMPLING_PERIOD_WHEN_DISARMED 10

// #define CHECK_DATA_FILESYSTEM

// decomment this line if you don't want the program to be closed on Loose Fencing due to I/O error.
#define EXIT_ON_LFR_IO

#define MIN_QUORUM_NODES 3
#define MAX_BUFFER_LEN 2000
#define MAX_LINE_LEN 256
#define MAX_REASONS_STRLEN 256
#define MAX_HOSTNAME_LEN 50 // 20

// eventually we'll dynamically read from the GPFS 'mmgetstate' bash command
// the result of the defined nodes
#define MAX_ALLOWED_SIMILAR_STAT_NODES 2 // it must be at least 2
// STATIC_ASSERT(MAX_ALLOWED_SIMILAR_STAT_NODES >= 2)
#define MAX_DEFINED_QUORUM_NODES 10
#define MAX_SNAPSHOTS 3 // 2

#define INTER_SNAPSHOTS_INTERVAL_SECONDS 3 // (MMGETSTATE_ELAPSED_SECONDS*SAFETY_SNAPSHOTS_INTERVAL_MULTIPLIER)
#define SNAPSHOT_TIMESTAMP_EPSILON 3 // 2 // 2 seconds of max difference

/*
in this macro we should define the path in which
the control file should be created and stat-ed
*/

#define GPFS_CONTROL_PATH "/data/.craafdd/quorum" //
#define GPFS_MMGETSTATE_COMMAND "mmgetstate -Ls" // + -N n001-ibj"
#define MAX_COMMAND_LEN 100
#define DEFAULT_PHYSICAL_QUORUM_CONDITION 0
#define MMGETSTATE_HOSTNAME_ROW_ID 3
#define MMGETSTATE_DEFINED_QUORUM_NODES_ROW_ID 10
#define MMGETSTATE_ACTIVE_QUORUM_NODES_ROW_ID  11
#define MMGETSTATE_MIN_QUORUM_NODES_ROW_ID 12
#define MMGETSTATE_DEFINED_NODES_LINE "Number of n" // odes defined in the cluster
#define MMGETSTATE_ACTIVE_NODES_LINE "Number of l" // ocal nodes active in the cluster
#define MMGETSTATE_DEFINED_QUORUM_NODES_LINE "Number of quorum nodes d" // efined in the cluster:"
#define MMGETSTATE_ACTIVE_QUORUM_NODES_LINE "Number of quorum nodes a" // ctive in the cluster:"
#define MMGETSTATE_MIN_QUORUM_NODES_LINE "Quorum ="

#define FENCING_COMMAND "/usr/lpp/mmfs/bin/mmshutdown"
#define FENCING_DAEMON_LOGFILE "craafdd_log.txt"
#define FENCING_DAEMON_BACKUP_FILEPNT stderr
#define AUDITED_NETWORK_INTERFACE "ib3" // "bond1" // "bond-bond1" //"eno1np0"

#define DEFAULT_LFR_ERROR_STATEMENT 0

#define DEFAULT_SLEEPING_SECONDS_AFTER_FENCING 0
#define DEFAULT_SLEEPING_SECONDS_AFTER_LFR_ERROR 0
#define DEFAULT_REMOVAL_BEHAVIOR_AFTER_FENCING 1
#define DEFAULT_REMOVAL_BEHAVIOR_AFTER_LFR_ERROR 1

#define DEFAULT_MAIL_COMMAND "sendmail"
#define DEFAULT_FROM_MAIL "craafdd"
#define MAX_MAILCMDTO_LEN 128

#define MAX_LOGBUF_LEN 1000
#define MAX_MAIL_BUF 130000 // 2000

typedef struct _nodestats
{
	char filename[PATH_MAX];
	unsigned long stat_timestamp;
} nodestats;

typedef enum _fr_int
{
	FR_IFF_NOFAULTS,
	FR_IFF_ADDR_IFF_RUNNING,
	FR_IFF_ADDR,
	FR_IFF_RUNNING,
	FR_IFF_UNKNOWN
} fr_int;

typedef enum _lfr_int
{
	LFR_NOFAULTS,
	LFR_FAILED_QUORUM,
	LFR_FAILED_QUORUM_CONCURRENCY,
	LFR_FAILED_LOCAL_QUORUM,
	LFR_FAILED_LOCAL_QUORUM_CONCURRENCY,
	LFR_FAILED_SNAPSHOTS_TEST,
	LFR_DISTRIBUTED_FAULT,
	LFR_IO,
	LFR_UNKNOWN
} lfr_int;

#define MAX_FAULT_REASONS FR_IFF_UNKNOWN
#define MAX_LOOSE_FENCING_REASONS LFR_UNKNOWN

FILE * logging_fp;
volatile unsigned char craafdd_exec;
volatile unsigned char is_daemon_armed;
volatile unsigned char fault_flag;

static inline void fencing_instruction(const char * fencing_command)
{
	(void) system(fencing_command);
	// sleep(10);
	return;
}

#define FENCING_INSTRUCTION fencing_instruction

#define CRAAFDD_SIGNAL_SIGINT SIGINT
#define CRAAFDD_SIGNAL_SIGTERM SIGTERM
#define CRAAFDD_SIGNAL_DAEMONREARM SIGUSR1
#define CRAAFDD_SIGNAL_FORCEFAULTMANAGEMENT SIGUSR2

static inline unsigned char file_exists(const char *filename)
{
        FILE *file = NULL;

        if((file = fopen(filename, "r")))
        {
                fclose(file);
                return 1;
        }

        return 0;
}

static inline const char * gettime(time_t * aux_time)
{
	static char buftime[MAX_BUFTIME_LENGTH];
        static time_t logging_timestamp;
        static struct tm ts;

	logging_timestamp = time(NULL);

	if(aux_time)
		*aux_time = logging_timestamp;

        ts = *localtime(&logging_timestamp);
        strftime(buftime, sizeof(buftime), "%Y-%m-%d-%H:%M:%S", &ts);
	return buftime;
}

static void sendmail(const char * my_hostname, const char * from_mail, const char * mail_cmd, const char * message)
{
	FILE * pp;
	static char buf[MAX_MAIL_BUF];

	sprintf(buf, "Subject: CRAAFDD Event on %s\n"
		     "From: %s\n"
		     "Mime-Version: 1.0\n"
		     "Content-Type: multipart/related; boundary=\"boundary-example\"; type=\"text/html\"\n"
		     "\n"
		     "--boundary-example\n"
		     "Content-Type: text/html; charset=ISO-8859-15\n"
		     "Content-Transfer-Encoding: 7bit\n"
		     "\n"
		     "<html>"
		     "<head>"
		     "<meta http-equiv=\"content-type\" content=\"text/html; charset=ISO-8859-15\">"
		     "</head>"
		     "<body bgcolor=\"#ffffff\" text=\"#000000\">"
		     "<font face=\"Courier New\">%s</font><br><br><br><br><br>"
		     "<img src=\"cid:craafdd\" alt=\"craafdd logo\" style=\"width: 25%; height: auto; \"><br>\n"
		     CRAAFDD_DESCRIPTION"<br><br>"
                     "Best Regards,<br>Marco Chiarelli.<br><br>"
		     "</body>\n"
		     "</html>\n"
		     "\n"
		     "--boundary-example\n"
		     "Content-Type: IMAGE/PNG;name=\"craafdd.png\"\n"
                     "Content-Transfer-Encoding: BASE64\n"
		     "Content-ID: <craafdd>\n"
		     "Content-Disposition: inline;\n"
		     "\n" 
		     #include "craafdd.png.base64.h"
		    "--boundary-example\n\n", my_hostname, from_mail, message);

	if((pp = popen(mail_cmd, "w")) == NULL)
        {
                // fencing_fault = LFR_IO;
                fprintf(logging_fp, "[%s] ERROR: Cannot send mail through the %s command.\n", gettime(NULL), mail_cmd);
                fflush(logging_fp);
		return;
	}

	// fwrite(message, 1, strlen(message)+1, pp);
	fprintf(pp, buf); // message);
	pclose(pp);
	return;
}

static void daemon_management(int signum)
{
	signal(CRAAFDD_SIGNAL_SIGINT, SIG_IGN);
        signal(CRAAFDD_SIGNAL_SIGTERM, SIG_IGN);
	signal(CRAAFDD_SIGNAL_DAEMONREARM, SIG_IGN);
	signal(CRAAFDD_SIGNAL_FORCEFAULTMANAGEMENT, SIG_IGN);

	if(signum == CRAAFDD_SIGNAL_SIGINT || signum == CRAAFDD_SIGNAL_SIGTERM)
	{
		craafdd_exec = 0;
		fprintf(logging_fp, "[%s] CRAAFDD Daemon shutdown invoked.\n", gettime(NULL));
	}
	else if(signum == CRAAFDD_SIGNAL_DAEMONREARM)
	{
		// static const char * status[2] = { "DISARMED", "ARMED" };
		is_daemon_armed = !is_daemon_armed;
        	fprintf(logging_fp, "[%s] CRAAFDD Daemon has been successfully %s.\n", gettime(NULL), is_daemon_armed ? "ARMED" : "DISARMED"); // status[is_daemon_armed]);
	}
	else
	{
		fault_flag = 0;
        	fprintf(logging_fp, "[%s] CRAAFDD Daemon Fault Management has been successfully forced.\n", gettime(NULL));
	}

	fflush(logging_fp);
	
	signal(CRAAFDD_SIGNAL_SIGINT, daemon_management);
	signal(CRAAFDD_SIGNAL_SIGTERM, daemon_management);
	signal(CRAAFDD_SIGNAL_DAEMONREARM, daemon_management);
	signal(CRAAFDD_SIGNAL_FORCEFAULTMANAGEMENT, daemon_management);
	return;
}

static int cc_snapshot(nodestats * this_quorum_nodes, const char * my_hostname, const char * gpfs_control_path, FILE * logging_fp) // , const char node_control_char)
{
	// CONCURRENCY CONTROL CODE	
	int i, j;
	errno = 0;
        DIR * dirp = opendir(gpfs_control_path);
	char path_buf[PATH_MAX];
	struct dirent * dp;
	struct stat stat_buf;
        char * file_entry = NULL;

	j = 0;

	#ifdef DEBUG_MODE
	fprintf(logging_fp, "| - - - - |\n");
	fflush(logging_fp);
	#endif

	for(i=0; dirp; ++i)
        {
        	errno = 0;
                if ((dp = readdir(dirp)) != NULL)
                {
                	if((file_entry = dp->d_name)[0] != '.' && strcmp(file_entry, my_hostname)) // == node_control_char && strcmp(file_entry, my_hostname))
                        {
                        	sprintf(path_buf, "%s/%s", gpfs_control_path, file_entry);
				errno = 0;
                                if(stat(path_buf, &stat_buf))
				{
					closedir(dirp);
					dirp = NULL;
					continue;
				}
                               	strcpy(this_quorum_nodes[j].filename, file_entry);
                               	this_quorum_nodes[j].stat_timestamp = ((long)stat_buf.st_mtim.tv_sec)*1000000000 + stat_buf.st_mtim.tv_nsec;
				#ifdef DEBUG_MODE
				fprintf(logging_fp, "\nthis_quorum_nodes[i]: %s\nfile_entry: %s\nthis_quorum[i].timestamp: %ld\nfile entry stat timestamp: %ld\n", this_quorum_nodes[j].filename, file_entry, this_quorum_nodes[j].stat_timestamp, stat_buf.st_mtim.tv_sec);
				#endif
				++ j;
			}
                }
                else
                {
                	closedir(dirp);
                        dirp = NULL;
                }
        }

	#ifdef DEBUG_MODE
	fprintf(logging_fp, "| - - - - |\n");
	fflush(logging_fp);
	#endif
	return errno;
}

int main(int argc, char *argv[])
{
	const unsigned char am_i_quorum = argc > 1 ? atoi(argv[1]) : DEFAULT_AMIQUORUM;
	signed char min_quorum_nodes = argc > 2 ? atoi(argv[2]) : DEFAULT_MIN_QUORUM_NODES;
	const unsigned char sampling_period = argc > 3 ? atoi(argv[3]) : SAMPLING_PERIOD;
	const unsigned char sampling_period_when_disarmed = argc > 4 ? atoi(argv[4]) : SAMPLING_PERIOD_WHEN_DISARMED;
	const unsigned char max_snapshots = argc > 5 ? atoi(argv[5]) : MAX_SNAPSHOTS;
	const unsigned char inter_snapshot_interval_seconds = argc > 6 ? atoi(argv[6]) : INTER_SNAPSHOTS_INTERVAL_SECONDS;
	const unsigned char snapshot_timestamp_epsilon = argc > 7 ? atoi(argv[7]) : SNAPSHOT_TIMESTAMP_EPSILON;
	const unsigned char max_allowed_similar_stat_nodes = argc > 8 ? atoi(argv[8]) :  MAX_ALLOWED_SIMILAR_STAT_NODES;
	const char * gpfs_control_path = argc > 9 ? argv[9] : GPFS_CONTROL_PATH;
        const char * gpfs_mmgetstate_command = argc > 10 ? argv[10] : GPFS_MMGETSTATE_COMMAND;
	const unsigned char physical_quorum_condition = argc > 11 ? atoi(argv[11]) : DEFAULT_PHYSICAL_QUORUM_CONDITION;
	
	char gpfs_mmgetstate_all_command[MAX_COMMAND_LEN];
	sprintf(gpfs_mmgetstate_all_command, "%s -a", gpfs_mmgetstate_command);

	const char * gpfs_mmgetstate_which_command = physical_quorum_condition ? gpfs_mmgetstate_all_command : gpfs_mmgetstate_command;

	const unsigned char mmgetstate_hostname_row_id = argc > 12 ? atoi(argv[12]) : MMGETSTATE_HOSTNAME_ROW_ID;
	const unsigned char mmgetstate_defined_nodes_row_id = mmgetstate_hostname_row_id+4;
	const unsigned char mmgetstate_active_nodes_row_id = mmgetstate_defined_nodes_row_id+1;
	const unsigned char mmgetstate_defined_quorum_nodes_row_id = mmgetstate_active_nodes_row_id+2; // argc > 7 ? atoi(argv[8]) : MMGETSTATE_DEFINED_QUORUM_NODES_ROW_ID;
	const unsigned char mmgetstate_active_quorum_nodes_row_id = mmgetstate_defined_quorum_nodes_row_id+1; // argc > 8 ? atoi(argv[9]) : MMGETSTATE_ACTIVE_QUORUM_NODES_ROW_ID;
	const unsigned char mmgetstate_min_quorum_nodes_row_id = mmgetstate_active_quorum_nodes_row_id+1; // argc > 9 ? atoi(argv[10]) : MMGETSTATE_MIN_QUORUM_NODES_ROW_ID;
	const char * mmgetstate_defined_nodes_line = argc > 13 ? argv[13] : MMGETSTATE_DEFINED_NODES_LINE;
        const char * mmgetstate_active_nodes_line = argc > 14 ? argv[14] : MMGETSTATE_ACTIVE_NODES_LINE;
	const char * mmgetstate_defined_quorum_nodes_line = argc > 15 ? argv[15] : MMGETSTATE_DEFINED_QUORUM_NODES_LINE;
	const char * mmgetstate_active_quorum_nodes_line = argc > 16 ? argv[16] : MMGETSTATE_ACTIVE_QUORUM_NODES_LINE;
	const char * mmgetstate_min_quorum_nodes_line = argc > 17 ? argv[17] : MMGETSTATE_MIN_QUORUM_NODES_LINE;
	const char * fencing_command = argc > 18 ? argv[18] : FENCING_COMMAND;
	const char * fencing_daemon_logfile = argc > 19 ? argv[19] : FENCING_DAEMON_LOGFILE;
	const char * audited_network_interface = argc > 20 ? argv[20] : AUDITED_NETWORK_INTERFACE;
	const unsigned char lfr_error_statement = argc > 21 ? atoi(argv[21]) : DEFAULT_LFR_ERROR_STATEMENT;
	const unsigned char sleeping_seconds_after_fencing = argc > 22 ? atoi(argv[22]) : DEFAULT_SLEEPING_SECONDS_AFTER_FENCING; 
	const unsigned char sleeping_seconds_after_lfr_error = argc > 23 ? atoi(argv[23]) : DEFAULT_SLEEPING_SECONDS_AFTER_LFR_ERROR;
	const unsigned char removal_behavior_after_fencing = argc > 24 ? atoi(argv[24]) : DEFAULT_REMOVAL_BEHAVIOR_AFTER_FENCING;
	const unsigned char removal_behavior_after_lfr_error = argc > 25 ? atoi(argv[25]) : DEFAULT_REMOVAL_BEHAVIOR_AFTER_LFR_ERROR;
	const char * mail_cmd = argc > 26 ? argv[26] : DEFAULT_MAIL_COMMAND;
	const char * from_mail = argc > 27 ? argv[27] : DEFAULT_FROM_MAIL;
	const char * to_mail = argc > 28 ? argv[28] : NULL;

	char mail_cmd_to[MAX_MAILCMDTO_LEN];
        
	if(to_mail)
		sprintf(mail_cmd_to, "%s \"%s\"", mail_cmd, to_mail);

	#define is_mail_active (from_mail && to_mail) //  && !am_i_quorum)

	#ifdef DEBUG_MODE
	printf("\nam_i_quorum: %d\nmin_quorum_nodes: %d\nsampling_period: %d\nsampling_period_when_disarmed: %d\nmax_snapshots: %d\ninter_snapshot_interval_seconds: %d\nsnapshot_timestamp_epsilon: %d\nmax_allowed_similar_stat_nodes: %d\ngpfs_control_path: %s\ngpfs_mmgetstate_command: %s\ngpfs_mmgetstate_all_command: %s\ngpfs_mmgetstate_which_command: %s\nphysical_quorum_condition: %d\nmmgetstate_hostname_row_id: %d\nmmgetstate_defined_nodes_row_id: %d\nmmgetstate_active_nodes_row_id: %d\nmmgetstate_defined_quorum_nodes_row_id: %d\nmmgetstate_active_quorum_nodes_row_id: %d\nmmgetstate_min_quorum_nodes_row_id: %d\nmmgetstate_defined_nodes_line: %s\nmmgetstate_active_nodes_line: %s\nmmgetstate_defined_quorum_nodes_line: %s\nmmgetstate_active_quorum_nodes_line: %s\nmmgetstate_min_quorum_nodes_line: %s\nfencing_command: %s\nfencing_daemon_logfile: %s\naudited_network_interface: %s\nlfr_error_statement: %d\nsleeping_seconds_after_fencing: %d\nsleeping_seconds_after_lfr_error: %d\nremoval_behavior_after_fencing: %d\nremoval_behavior_after_lfr_error: %d\nmail_cmd: %s\nfrom_mail: %s\nto_mail: %s\nmail_cmd_to: %s\n\n", am_i_quorum, min_quorum_nodes, sampling_period, sampling_period_when_disarmed, max_snapshots, inter_snapshot_interval_seconds, snapshot_timestamp_epsilon, max_allowed_similar_stat_nodes, gpfs_control_path, gpfs_mmgetstate_command, gpfs_mmgetstate_all_command, gpfs_mmgetstate_which_command, physical_quorum_condition, mmgetstate_hostname_row_id, mmgetstate_defined_nodes_row_id, mmgetstate_active_nodes_row_id, mmgetstate_defined_quorum_nodes_row_id, mmgetstate_active_quorum_nodes_row_id, mmgetstate_min_quorum_nodes_row_id, mmgetstate_defined_nodes_line, mmgetstate_active_nodes_line, mmgetstate_defined_quorum_nodes_line, mmgetstate_active_quorum_nodes_line, mmgetstate_min_quorum_nodes_line, fencing_command, fencing_daemon_logfile, audited_network_interface, lfr_error_statement, sleeping_seconds_after_fencing, sleeping_seconds_after_lfr_error, removal_behavior_after_fencing, removal_behavior_after_lfr_error, mail_cmd, from_mail, to_mail, mail_cmd_to);
	
	/*
	sendmail("mibsa", from_mail, mail_cmd_to, "<br><br><br>"
			      "LINE 1<br>"
			      "LINE 2<br>"
			      "LINE 3<br>"
			      "prova da craafdd x2<br>");
	*/		      
			      
	printf("gettime: %s\n", gettime(NULL));
	sleep(3);
	printf("gettime: %s\n", gettime(NULL));

	exit(0);
	#endif

	int fd;
        struct ifreq ifr;
	char my_hostname[MAX_HOSTNAME_LEN];
	char logging_buf[MAX_LOGBUF_LEN];

	int i, j, k;
	int retval;
	int retval2;
	int retval_cc;
	// FILE * logging_fp; //  = stdout;
	FILE * fp; // control file
	char buftime[MAX_BUFTIME_LENGTH];
	char my_path_buf[PATH_MAX];
	struct stat my_stat_buf;
	unsigned long my_stat_timestamp;
	// char * curr_addr = NULL;
	struct tm ts;
	unsigned char fault_reason = 0;
	fault_flag = 0; //0 false, 1 true
	unsigned char fencing_fault = 0;
	unsigned char snap_test_flag = 0;
	const unsigned char mmgetstate_defined_nodes_line_len = strlen(mmgetstate_defined_nodes_line);
        const unsigned char mmgetstate_active_nodes_line_len = strlen(mmgetstate_active_nodes_line);
	const unsigned char mmgetstate_defined_quorum_nodes_line_len = strlen(mmgetstate_defined_quorum_nodes_line);
	const unsigned char mmgetstate_active_quorum_nodes_line_len = strlen(mmgetstate_active_quorum_nodes_line);
	const unsigned char mmgetstate_min_quorum_nodes_line_len = strlen(mmgetstate_min_quorum_nodes_line);
	unsigned char defined_quorum_nodes = MAX_DEFINED_QUORUM_NODES; // initial value before GPFS's mmgetstate command
	unsigned char active_quorum_nodes;
	unsigned char alone_in_the_dark = 1;
	// unsigned char min_quorum_nodes;
	// int my_hostname_len = 0; 
	int error_code = 0;

	// time_t logging_timestamp;
	time_t time_0;
	time_t time_1, time_2, time_3;
	time_t time_end;
	
	register unsigned char pass_timestamp;
	register unsigned char pass_timestamp_twins;
	register unsigned char found_my_hostname;

	const char * fault_reason_strings[MAX_FAULT_REASONS] =
	{
		"Unknown",
        	"Missing Interface Address and Detached Link",
        	"Missing Interface",
        	"Detached Link"
	};

	const char * fencing_fault_reason_strings[MAX_LOOSE_FENCING_REASONS] =
	{
		"Unknown",
		"Failed Quorum",
		"Failed Quorum (concurrency)",
		"Failed Local Quorum",
		"Failed Local Quorum (concurrency)",
		"Failed Snapshots Test",
		"Distributed Fault",
		"I/O"
	};

	// FAULT DETECTION VARIABLES
	char * pnt;
        char * token;
	char * token2;
	const char * buft;
        char buf[MAX_LINE_LEN];
        FILE * pp;
	int line_num;
	//

	 // ***************************************************
        // HOSTNAME CONSTRUCTION CODE   
        /*
        We'll investigate whether this part could be dynamically
        evaluated into the fault detection code
        */ 
	// #ifdef DEBUG_MODE
        /* (void) gethostname(my_hostname, MAX_HOSTNAME_LEN);*/
        // printf("my_hostname: %s\n", my_hostname);
        /* strchr(my_hostname, '.')[0]='\0'; */
        // char new_my_hostname[MAX_HOSTNAME_LEN];      
        // printf("stripped my_hostname: %s\n", my_hostname);
        /*strcat(my_hostname, HOSTNAME_SUFFIX);*/
        // if my node name begins with 's', then
        // it is likely that all node names begins with 's'
        // const char node_control_char = my_hostname[0];
        /*
        printf("new my_hostname: %s\n", my_hostname);
        exit(0);
        #endif
        */
	// ***************************************************


	fault_flag = 0; //0 false, 1 true
	is_daemon_armed = 1;
	
	signal(CRAAFDD_SIGNAL_SIGINT, daemon_management);
	signal(CRAAFDD_SIGNAL_SIGTERM, daemon_management);
	signal(CRAAFDD_SIGNAL_DAEMONREARM, daemon_management);
	signal(CRAAFDD_SIGNAL_FORCEFAULTMANAGEMENT, daemon_management);

	// eventually open the logging file descriptor
	if((logging_fp = fopen(fencing_daemon_logfile, "a+")) == NULL)
	{
		 fprintf(FENCING_DAEMON_BACKUP_FILEPNT, "[%s] FATAL ERROR: %s. errno = %d. Cannot open logfile.\n", gettime(NULL), fencing_fault_reason_strings[fencing_fault=LFR_IO], errno);
                 #ifdef EXIT_ON_LFR_IO
                 exit(1);
                 #endif
	}

	fprintf(logging_fp, CRAAFDD_DESCRIPTION"\n");
	fprintf(logging_fp, "[%s] START LOGGING\n\n", gettime(&time_0));
	fflush(logging_fp);

	#ifdef DEBUG_MODE
	fclose(logging_fp);
	exit(0);
	#endif

	errno = 0;

	// GPFS' mmgetstate command output parsing 

	if((pp = popen(gpfs_mmgetstate_command, "r")) == NULL)
        {
        	// fencing_fault = LFR_IO;
                fprintf(logging_fp, "[%s] FATAL ERROR: %s. errno = %d. Cannot gather hostname through \'%s\' command.\n", gettime(NULL), fencing_fault_reason_strings[fencing_fault=LFR_IO], errno, gpfs_mmgetstate_command);
                fflush(logging_fp);
                #ifdef EXIT_ON_LFR_IO
                fclose(logging_fp);
                exit(1);
                #endif
        } 

	sleep(1);	

	for(line_num=0; fgets(buf, MAX_LINE_LEN, pp); ++line_num)
        {
        	//printf("LINE: %s\n", buf);

                if(line_num == mmgetstate_hostname_row_id)
                {
                	for(token=buf; *token == ' ' || isdigit(*token) && *(token+1) == ' '; ++token);
                        for(pnt=token; *pnt != ' '; ++pnt);
                        *pnt='\0';
                        strcpy(my_hostname, token);
			// my_hostname_len = strlen(my_hostname);
                        #ifdef DEBUG_MODE
                        printf("my hostname: %s\n", my_hostname);
                        exit(0);
                        #endif
                        break;
                }
	}

	//strcpy(my_hostname, "ems1a8-ibt2");
	// strcpy(my_hostname, "ess3a9-ibt2"); // "ess2a8-ibt2"); // "ess3a9-ibt2"); // "ess1a8-ibt2"); // "ems1a8-ibt2"); // "ess1a8-ibt2"); // "ess3a9-ibt2");

	sprintf(my_path_buf, "%s/%s", gpfs_control_path, my_hostname);
	remove(my_path_buf);
	pclose(pp);

	#ifdef CHECK_DATA_FILESYSTEM
	errno = 0;

	if((fp = fopen(my_path_buf, "w")) == NULL)
        {
        	fprintf(logging_fp, "[%s] FATAL ERROR: %s. errno = %d. Cannot access GPFS Shared Filesystem.\n", gettime(NULL), fencing_fault_reason_strings[fencing_fault=LFR_IO], errno);
		fflush(logging_fp);
		#ifdef EXIT_ON_LFR_IO
		fclose(logging_fp);
		exit(1);
		#endif
	}
	
	fclose(fp);
	remove(my_path_buf);
	#endif

	errno = 0;

	if((fd = socket(AF_INET, SOCK_DGRAM, 0)) == -1)
	{
		fprintf(logging_fp, "[%s] FATAL ERROR: %s. errno = %d. Cannot open UDP socket.\n", gettime(NULL), fencing_fault_reason_strings[fencing_fault=LFR_IO], errno);
        	fflush(logging_fp);
                #ifdef EXIT_ON_LFR_IO
                fclose(logging_fp);
                exit(1);
                #endif
	}

	for( craafdd_exec = 1 ; craafdd_exec ; )
	{
		if(!is_daemon_armed)
		{
			sleep(sampling_period_when_disarmed);
			continue;
		}

		else if(sampling_period)
			sleep(sampling_period);

		//fd = socket(AF_INET, SOCK_DGRAM, 0);
        	//Type of address to retrieve - IPv4 IP address
        	ifr.ifr_addr.sa_family = AF_INET;
        	//Copy the interface name in the ifreq structure
        	strncpy(ifr.ifr_name , audited_network_interface , IFNAMSIZ-1);
		//this first call to ioctl tries to gather the mapped IP on the auditable interface
        	retval = ioctl(fd, SIOCGIFADDR, &ifr);
        	//close(fd);
		//this second call to ioctl does link probing
		retval2 = ioctl(fd, SIOCGIFFLAGS, &ifr);
		fault_reason = FR_IFF_NOFAULTS;

		#ifdef DEBUG_MODE
		if(!retval2)
		{
			if ((ifr.ifr_flags & IFF_UP) == IFF_UP)
				printf("flag IFF_UP true\n");
			if ((ifr.ifr_flags & IFF_RUNNING) == IFF_RUNNING)
				printf("flag IFF_RUNNING true\n");

			// exit(0);
		}

		exit(0);
		#endif

        	//display result
		// decomment the line below if you want to obtain the mapped IP on the auditable interface
		/*
		//curr_addr = inet_ntoa(( (struct sockaddr_in *)&ifr.ifr_addr )->sin_addr);
		//printf("%s - %s\n" , audited_network_interface , curr_addr);

		#ifdef DEBUG_MODE
		fprintf(logging_fp, "%s\n", curr_addr);
		#endif
		*/

		if(retval && ((!retval2) && ((ifr.ifr_flags & IFF_RUNNING) != IFF_RUNNING)))
			fault_reason = FR_IFF_ADDR_IFF_RUNNING;
		else
		{
			if(retval)
                        	fault_reason = FR_IFF_ADDR;

                	if((!retval2) && ((ifr.ifr_flags & IFF_RUNNING) != IFF_RUNNING))
                        	fault_reason = FR_IFF_RUNNING;
		}

		if (fault_reason) //  || (curr_addr[0] ==  '0' && curr_addr[2] == '0' && curr_addr[4] == '0' && curr_addr[6] == '0'))
		{
			#ifdef DEBUG_MODE
			printf("entered in fault detection code\n");
			#endif
			if (!fault_flag)
			{
				errno = 0;
				fault_flag = 1;
			
                       
                     		buft = gettime(&time_1); 
				sprintf(logging_buf, "<b>[<font color=\"blue\">%s</font> - %s]</b> <b><font color=\"red\">EVENT</font></b>: interface <font color=\"red\">\"%s\"</font> is down. <b><font color=\"red\">REASONS</font></b>: %s. Arbitrating..<br>", my_hostname, buft, audited_network_interface, fault_reason_strings[fault_reason]);	
				fprintf(logging_fp, "[%s] EVENT: interface \"%s\" is down. REASONS: %s. Arbitrating..\n", buft, audited_network_interface, fault_reason_strings[fault_reason]);



				#ifdef DEBUG_MODE
                                printf("Logging time: %s\n", buftime);
                                #endif

				// already done in initializing step..
				// sprintf(my_path_buf, GPFS_CONTROL_PATH "/%s", my_hostname); 
				if((fp = fopen(my_path_buf, "w")) == NULL)
				{
					buft = gettime(NULL);
				sprintf(logging_buf, "%s<b>[%s]</b> <b><font color=\"red\">ERROR</font></b>: %s. Cannot write Status File. errno = %d. <font color=\"red\">Loose Fencing</font>.<br>", logging_buf, buft, fencing_fault_reason_strings[fencing_fault=LFR_IO], errno);

					fprintf(logging_fp, "[%s] ERROR: %s. Cannot write Status File. errno = %d. Loose Fencing.\n", buft, fencing_fault_reason_strings[fencing_fault=LFR_IO], errno);
					
					if(sleeping_seconds_after_lfr_error)
                                        	sleep(sleeping_seconds_after_lfr_error);

					if(lfr_error_statement)
					{
						fflush(logging_fp);

						if(is_mail_active)
							sendmail(my_hostname, from_mail, mail_cmd_to, logging_buf);

						// fclose(logging_fp);
						continue;
					}

					error_code = 1;
					break;	
				}

				fclose(fp);
				errno = 0;

				if(stat(my_path_buf, &my_stat_buf))
				{
					// fencing_fault = LFR_IO;
					buft = gettime(NULL);
					sprintf(logging_buf, "%s<b>[%s]</b> <b><font color=\"red\">ERROR</font></b>: %s. Cannot get Status File stat information. errno = %d. <font color=\"red\">Loose Fencing</font>.<br>", logging_buf, buft, fencing_fault_reason_strings[fencing_fault=LFR_IO], errno);
					fprintf(logging_fp, "[%s] ERROR: %s. Cannot get Status File stat information. errno = %d. Loose Fencing.\n", buft, fencing_fault_reason_strings[fencing_fault=LFR_IO], errno);	
					
					if(sleeping_seconds_after_lfr_error)
                                        	sleep(sleeping_seconds_after_lfr_error);
					
					if(removal_behavior_after_lfr_error)
						remove(my_path_buf);
					
					if(lfr_error_statement)
					{
						fflush(logging_fp);	

						if(is_mail_active)
                                                        sendmail(my_hostname, from_mail, mail_cmd_to, logging_buf);

						// fclose(logging_fp);
						continue;
					}
						
					error_code = 1;
					break;	
				}

				my_stat_timestamp = ((long) my_stat_buf.st_mtim.tv_sec)*1000000000 + my_stat_buf.st_mtim.tv_nsec;

				// fault_flag = 1;

				#ifdef DEBUG_MODE
				fflush(logging_fp);
				exit(0);
				#endif

				fflush(logging_fp);
				// continue;

				// GPFS' mmgetstate command output parsing
				pnt = NULL;
				errno = 0;

        			if((pp = popen(gpfs_mmgetstate_which_command, "r")) == NULL)
				{
					// fencing_fault = LFR_IO;
					buft = gettime(NULL);
					sprintf(logging_buf, "%s<b>[%s]</b> <b><font color=\"red\">ERROR</font></b>: %s. Cannot invoke \"%s\" command. <font color=\"red\">Loose Fencing</font>.<br>", logging_buf, buft, fencing_fault_reason_strings[fencing_fault=LFR_IO], gpfs_mmgetstate_command); 
					fprintf(logging_fp, "[%s] ERROR: %s. Cannot invoke \"%s\" command. Loose Fencing.\n", buft, fencing_fault_reason_strings[fencing_fault=LFR_IO], gpfs_mmgetstate_command); 
					
					if(sleeping_seconds_after_lfr_error)
                                        	sleep(sleeping_seconds_after_lfr_error);

                                        if(removal_behavior_after_lfr_error)
						remove(my_path_buf);
                                       
					if(lfr_error_statement)
                                        {
                                        	fflush(logging_fp);

						if(is_mail_active)
                                                        sendmail(my_hostname, from_mail, mail_cmd_to, logging_buf);

						// fclose(logging_fp);
                                      		continue;
					}

					error_code = 1;
					break;

				}

				line_num = 0;
				found_my_hostname = 0;;
				active_quorum_nodes = 0;
				defined_quorum_nodes = 0;
  				

				if(physical_quorum_condition)
				{
					alone_in_the_dark = 1;
					for(line_num=0; fgets(buf, MAX_LINE_LEN, pp); ++line_num)
                                        {
                                                //printf("LINE: %s\n", buf);
						if((!am_i_quorum) && !strncmp(buf, mmgetstate_defined_nodes_line, mmgetstate_defined_nodes_line_len))
                                                        defined_quorum_nodes = atoi(strrchr(buf, ':')+1);
                                                else if((!am_i_quorum) && !strncmp(buf, mmgetstate_active_nodes_line, mmgetstate_active_nodes_line_len))
                                                        active_quorum_nodes = atoi(strrchr(buf, ':')+1);
						else if(!strncmp(buf, mmgetstate_defined_quorum_nodes_line, mmgetstate_defined_quorum_nodes_line_len))
                                                        defined_quorum_nodes = (am_i_quorum ? atoi(strrchr(buf, ':')+1) : (defined_quorum_nodes-atoi(strrchr(buf, ':')+1))) -1;
                                                else if(!strncmp(buf, mmgetstate_active_quorum_nodes_line, mmgetstate_active_quorum_nodes_line_len))
                                                        active_quorum_nodes = am_i_quorum ? atoi(strrchr(buf, ':')+1) : (active_quorum_nodes-atoi(strrchr(buf, ':')+1));
                                                else if(min_quorum_nodes < 0 && !strncmp(buf, mmgetstate_min_quorum_nodes_line, mmgetstate_min_quorum_nodes_line_len))
                                                        min_quorum_nodes = atoi(strrchr(buf, '=')+1);
						else if(alone_in_the_dark && line_num >= mmgetstate_hostname_row_id)
						{
							#ifdef DEBUG_MODE
							fprintf(logging_fp, "\n\n--------------\n\n");

							fprintf(logging_fp, buf);
							#endif

							for(token=buf; (*token == ' ' || (isdigit(*token) && *(token+1) == ' ')) && *token != '\0'; ++token);
							 
							#ifdef DEBUG_MODE
							fprintf(logging_fp, "for1 token: %s\n", token);
							#endif

							if(!found_my_hostname)
						        {
								for(pnt=token,token2=my_hostname; *pnt == *token2 && *pnt != '\0' && *token2 != '\0'; ++pnt, ++token2);
							 
							 	#ifdef DEBUG_MODE
							 	fprintf(logging_fp, "for2 pnt: %s\n", pnt);
							 	fprintf(logging_fp, "for2 token2: %s\n", token2);
							 	#endif

							 	if(*token2 == '\0')
								{
									found_my_hostname = 1;
									continue;
                        				 	}
							}

							for(pnt=token; (!isdigit(*pnt)) && *pnt != '\0'; ++pnt);

							#ifdef DEBUG_MODE
							fprintf(logging_fp, "for3 pnt: %s\n", pnt);
							#endif

							for(token2=my_hostname; (!isdigit(*token2)) && *token2 != '\0'; ++token2);

							#ifdef DEBUG_MODE
							fprintf(logging_fp, "for4 token2: %s\n", token2);
							#endif

							for( ; isdigit(*token2) && *token2 != '\0'; ++token2);
							 
							#ifdef DEBUG_MODE
							fprintf(logging_fp, "for5 token2: %s\n", token2);
							#endif
							 
							for( ; isdigit(*pnt) && *pnt != '\0'; ++pnt);
						  	 
							#ifdef DEBUG_MODE
							fprintf(logging_fp, "for6 pnt: %s\n", pnt);
							#endif

							for( ; *pnt == *token2 && *pnt != '\0' && *token2 != '\0'; ++pnt, ++token2)
								if(*pnt == '-')
								{
									for( ; *pnt != '\n'; ++ pnt)
										if(*pnt == 'a')
										{	 
									 		alone_in_the_dark = 0;
											break;
										}
									   	else if(*pnt == 'd')
									  		break;

									break;
								}

							#ifdef DEBUG_MODE
							fprintf(logging_fp, "for7 pnt: %s\n", pnt);
							fprintf(logging_fp, "for7 token2: %s\n", token2);
							fprintf(logging_fp, "\n\n--------------\n\n");
							#endif

							// *pnt='\0';

						}
                                        }
				}
				else 
				{
					alone_in_the_dark = 0;
					for(line_num=0; fgets(buf, MAX_LINE_LEN, pp); ++line_num)
					{
						//printf("LINE: %s\n", buf);

						if((!am_i_quorum) && line_num == mmgetstate_defined_nodes_row_id)
                                                        defined_quorum_nodes = atoi(strrchr(buf, ':')+1);
                                                else if((!am_i_quorum) && line_num == mmgetstate_active_nodes_row_id)
                                                        active_quorum_nodes = atoi(strrchr(buf, ':')+1);
						else if(line_num == mmgetstate_defined_quorum_nodes_row_id)
							defined_quorum_nodes = (am_i_quorum ? atoi(strrchr(buf, ':')+1) : (defined_quorum_nodes-atoi(strrchr(buf, ':')+1))) -1;
						else if(line_num == mmgetstate_active_quorum_nodes_row_id)
							active_quorum_nodes = am_i_quorum ? atoi(strrchr(buf, ':')+1) : (active_quorum_nodes-atoi(strrchr(buf, ':')+1));
						else if(min_quorum_nodes < 0 && line_num == mmgetstate_min_quorum_nodes_row_id)
							min_quorum_nodes = atoi(strrchr(buf, '=')+1);
					}
				}

				pclose(pp);
				
				#ifdef DEBUG_MODE
                                fprintf(logging_fp, "\n\nAlone in the dark: %d\nDefined Quorum Nodes: %d\nActive Quorum Nodes: %d\nQuorum nodes: %d\n", alone_in_the_dark, defined_quorum_nodes, active_quorum_nodes, min_quorum_nodes);
                                fflush(logging_fp);
                                exit(0);
                                #endif

				/*
				Ricordiamo che inizialmente avevamo un -1 (per tenere conto di me stesso che sta per fare fencing)
				nell'assegnazione di active_nodes all'interno dell'if strncmp.
				Nell'if di cui sotto invece, avevamo messo un +1 per far sì che la differenza fosse sempre strettamente maggiore di 0.
				L'equivalente sarebbe: (active_nodes-1) >= quorum_nodes, che è equivalente a (active_nodes-1) > (quorum_nodes-1).
				Sommando membro a membro un +1, troviamo l'espressione equivalente di cui sotto.
				*/

				fencing_fault = 0;

				if((!alone_in_the_dark) && active_quorum_nodes > min_quorum_nodes) //  >= MIN_QUORUM_NODES)
				{
                                	nodestats quorum_nodes[2][defined_quorum_nodes];		

                			for(j=0; j<defined_quorum_nodes; ++j)
                			{			
                        			sprintf(quorum_nodes[0][j].filename, " ");
                        			quorum_nodes[0][j].stat_timestamp = 0;
						sprintf(quorum_nodes[1][j].filename, " ");
                                                quorum_nodes[1][j].stat_timestamp = 0;
                			}

					snap_test_flag = 0;
					register unsigned char snap_cnt = 0;
					register unsigned char last_snapshot_idx;

					for(i=0; i<max_snapshots; ++i)
					{
						sleep(inter_snapshot_interval_seconds);
						last_snapshot_idx = (int)(i%2);
						
						if(cc_snapshot(quorum_nodes[last_snapshot_idx], my_hostname, gpfs_control_path, logging_fp)) // , node_control_char))
						{
							fencing_fault = LFR_IO;
							break;
						}
			
						#ifdef DEBUG_MODE				
						for(j=0; j<defined_quorum_nodes; ++j)
							fprintf(logging_fp, "\nquorum_nodes[%d][%d].filename: %s\n, quorum_nodes[%d][%d].stat_timestamp: %ld\n\n",
										last_snapshot_idx, j, quorum_nodes[last_snapshot_idx][j].filename, last_snapshot_idx, j, quorum_nodes[last_snapshot_idx][j].stat_timestamp);
						#endif
						
						// snap_test_flag = 0;

						if(i)
						{
							for(j=0; j<defined_quorum_nodes; ++j)
								for(k=0; k<defined_quorum_nodes; ++k)
									//if((quorum_nodes[0][j].stat_timestamp*quorum_nodes[1][k].stat_timestamp) &&
										if((!strcmp(quorum_nodes[0][j].filename, quorum_nodes[1][k].filename)) &&
										(quorum_nodes[0][j].stat_timestamp == quorum_nodes[1][k].stat_timestamp))
											++ snap_cnt;

							#ifdef DEBUG_MODE
							fprintf(logging_fp, "\nsnap_cnt: %d\n", snap_cnt);
							#endif

							if((snap_test_flag = (snap_cnt >= defined_quorum_nodes)))
                                                        	break; 
						}	

						
						// if(max_snapshots -1 -i)
						//	sleep(inter_snapshot_interval_seconds);
						
					}

					snap_test_flag = 1;

					if(!fencing_fault)
					{
						register time_t stat_timestamp;
						snap_cnt = 0;

						if(snap_test_flag)
						{	
							pass_timestamp = 1;
							pass_timestamp_twins = 1;
							
							// COMPARISON BETWEEN ME AND OTHER DAEMONS FILE STATS
							for(i=0; i<defined_quorum_nodes; ++i)
								if((stat_timestamp = quorum_nodes[last_snapshot_idx][i].stat_timestamp) && strcmp(quorum_nodes[last_snapshot_idx][i].filename, my_hostname))
								#ifdef DEBUG_MODE
								{
									fprintf(logging_fp, "\nmy_stat_timestamp: %ld\nstat_timestamp: %ld\n", my_stat_timestamp, stat_timestamp);
								#endif
									//if((fabs(my_stat_timestamp - stat_timestamp)/1000000000) <= snapshot_timestamp_epsilon)
									if(((unsigned long)(labs(my_stat_timestamp - stat_timestamp)/1000000000)) <= snapshot_timestamp_epsilon)
									{
										#ifdef DEBUG_MODE
											fprintf(logging_fp, "\nsnap_cnt increment\n");
										#endif
								
										if(pass_timestamp && pass_timestamp_twins && stat_timestamp < my_stat_timestamp)
                                                                                        pass_timestamp = 0;


										// LOGICA DI GESTIONE GEMELLO
										if(physical_quorum_condition && pass_timestamp_twins)
										{
											/*
											for(pnt=quorum_nodes[last_snapshot_idx][i].filename,token2=my_hostname; *pnt == *token2 && *pnt != '\0' && *token2 != '\0'; ++pnt, ++token2);

                                                         				#ifdef DEBUG_MODE
                                                         				fprintf(logging_fp, "for2 pnt: %s\n", pnt);
                                                         				fprintf(logging_fp, "for2 token2: %s\n", token2);
                                                         				#endif
											*/

											for(pnt=quorum_nodes[last_snapshot_idx][i].filename; (!isdigit(*pnt)) && *pnt != '\0'; ++pnt);

                                                        				#ifdef DEBUG_MODE
                                                        				fprintf(logging_fp, "for pnt: %s\n", pnt);
                                                        				#endif

                                                        				for(token2=my_hostname; (!isdigit(*token2)) && *token2 != '\0'; ++token2);

                                                        				#ifdef DEBUG_MODE
                                                        				fprintf(logging_fp, "for2 token2: %s\n", token2);
                                                        				#endif

											/*
                                                         				if(*token2 == '\0')
                                                                 				continue;
											*/

                                                         				for( ; isdigit(*token2) && *token2 != '\0'; ++token2);

                                                         				#ifdef DEBUG_MODE
                                                         				fprintf(logging_fp, "for3 token2: %s\n", token2);
                                                         				#endif

                                                         				for( ; isdigit(*pnt) && *pnt != '\0'; ++pnt);

                                                         				#ifdef DEBUG_MODE
                                                         				fprintf(logging_fp, "for4 pnt: %s\n", pnt);
                                                         				#endif

                                                         				for( ; *pnt == *token2 && *pnt != '\0' && *token2 != '\0'; ++pnt, ++token2)
                                                                 				if(*pnt == '-')
                                                                 				{
													// if(stat_timestamp < my_stat_timestamp) // my_stat_timestamp)
                                                                                        			pass_timestamp_twins = 0;

													break;
												}

                                                         				#ifdef DEBUG_MODE
                                                         				fprintf(logging_fp, "for5 pnt: %s\n", pnt);
                                                         				fprintf(logging_fp, "for5 token2: %s\n", token2);
                                                         				fprintf(logging_fp, "\n\n--------------\n\n");
                                                         				#endif
										// END LOGICA DI GESTIONE GEMELLO
										}


										++ snap_cnt;
									}
							#ifdef DEBUG_MODE
								}

							fprintf(logging_fp, "\nsnap_cnt: %d\n", snap_cnt);
							fflush(logging_fp);
							#endif

							// CHECK WHETHER THE FAULT IS NOT DISTRIBUTED AMONG ALL THE QUORUM NODES O
							if(snap_cnt <= max_allowed_similar_stat_nodes -1)
							{
								#ifdef DEBUG_MODE
								fprintf(logging_fp, "active_quorum_nodes: %d\nmin_quorum_nodes: %d\nsnap_cnt: %d\nmy_stat_timestamp: %lu\n", //minimum_timestamp: %lu\n", 
											active_quorum_nodes, min_quorum_nodes, snap_cnt, my_stat_timestamp); // , minimum_timestamp);
								fflush(logging_fp);
								#endif

								if(!pass_timestamp_twins)
									fencing_fault = LFR_FAILED_LOCAL_QUORUM_CONCURRENCY;
								else if(active_quorum_nodes - snap_cnt <= min_quorum_nodes && !pass_timestamp) // active_quorum_nodes == min_quorum_nodes+1 && snap_cnt == min_quorum_nodes && !pass_timestamp)
									fencing_fault = LFR_FAILED_QUORUM_CONCURRENCY;
								else
								{
									// FENCING INSTRUCTION
									buft = gettime(&time_2);
									sprintf(logging_buf, "%s<b>[%s]</b> \"%s\" command invoked.<br>", logging_buf, buft, fencing_command);
									fprintf(logging_fp, "[%s] \"%s\" command invoked.\n", buft, fencing_command);
									fflush(logging_fp);
									FENCING_INSTRUCTION(fencing_command);
									buft = gettime(&time_3);
									time_3 -= time_2;
									time_2 -= time_1;
									time_1 = time_2+time_3;
									sprintf(logging_buf, "%s<b>[%s]</b> <font color=\"green\">Fencing completed</font>.<br><br><b>Arbitration Elapsed Time</b>: %ld seconds;<br><b>\"%s\" command Elapsed Time</b>: %ld seconds;<br><b>Total Elapsed Time</b>: %ld seconds.<br><br><br>", logging_buf, buft, time_2, fencing_command, time_3, time_1);
									fprintf(logging_fp, "[%s] Fencing completed.\nArbitration Elapsed Time: %ld seconds, \"%s\" command Elapsed Time: %ld seconds, Total Elapsed Time: %ld seconds\n", buft, time_2, fencing_command, time_3, time_1);
									fflush(logging_fp);

									errno = 0;

        								// GPFS' mmgetstate command output parsing 

        								if((pp = popen(gpfs_mmgetstate_all_command, "r")) == NULL)
        								{
                								// fencing_fault = LFR_IO;
                								fprintf(logging_fp, "[%s] ERROR: %s. errno = %d. Cannot invoke \'%s\' command.\n", gettime(NULL), fencing_fault_reason_strings[fencing_fault=LFR_IO], errno, gpfs_mmgetstate_all_command);
                								fflush(logging_fp);
        								}
									else
									{
        									sleep(1);
										sprintf(logging_buf, "%s<p style=\"background-color: black;\"><font color=#adff29>", logging_buf);
        									for( ; fgets(buf, MAX_LINE_LEN, pp); )
											sprintf(logging_buf, "%s%s<br>", logging_buf, buf);
                									//printf("LINE: %s\n", buf);
										sprintf(logging_buf, "%s</font></p><br>", logging_buf);
										pclose(pp);
									}

 									errno = 0;

									if(is_mail_active)
										sendmail(my_hostname, from_mail, mail_cmd_to, logging_buf);

									#ifdef DEBUG_MODE
									fprintf(logging_fp, "logging_buf is %s\n", logging_buf);
									fflush(logging_fp);
									#endif

									if(sleeping_seconds_after_fencing)
                                                        			sleep(sleeping_seconds_after_fencing);

									if(removal_behavior_after_fencing)
										remove(my_path_buf);
								}

							}
							else
								fencing_fault = LFR_DISTRIBUTED_FAULT;
						}
						else
							fencing_fault = LFR_FAILED_SNAPSHOTS_TEST;
					}	
	
				}
				else
					fencing_fault = alone_in_the_dark ? LFR_FAILED_LOCAL_QUORUM : LFR_FAILED_QUORUM;

				if(fencing_fault)
				{	
					buft = gettime(&time_2);
					time_2 -= time_1;
					sprintf(logging_buf, "%s<b>[%s]</b> <font color=\"red\">Loose Fencing. <b>REASON</b></font>: %s.<br><b>Arbitration Elapsed Time</b>: %ld seconds.<br><br><br>", logging_buf, buft, fencing_fault_reason_strings[fencing_fault], time_2);
					fprintf(logging_fp, "[%s] Loose Fencing. REASON: %s.\nArbitration Elapsed Time: %ld seconds.\n", buft, fencing_fault_reason_strings[fencing_fault], time_2);
					
					errno = 0;

                                        // GPFS' mmgetstate command output parsing 

                                        if((pp = popen(gpfs_mmgetstate_all_command, "r")) == NULL)
                                        {
                                        	// fencing_fault = LFR_IO;
                                                fprintf(logging_fp, "[%s] ERROR: %s. errno = %d. Cannot invoke \'%s\' command.\n", gettime(NULL), fencing_fault_reason_strings[fencing_fault=LFR_IO], errno, gpfs_mmgetstate_all_command);
                                                fflush(logging_fp);
                                        }
                                        else
                                        {
	                                	sleep(1);
						sprintf(logging_buf, "%s<p style=\"background-color: black;\"><font color=#adff29>", logging_buf);
         	                        	for( ; fgets(buf, MAX_LINE_LEN, pp); )
                                                	sprintf(logging_buf, "%s%s<br>", logging_buf, buf);
                                                        //printf("LINE: %s\n", buf);
						sprintf(logging_buf, "%s</font></p><br>", logging_buf);
                                        	pclose(pp);
                                        }

                                        errno = 0;

					if(sleeping_seconds_after_lfr_error)
                                                sleep(sleeping_seconds_after_lfr_error);

                                        if(removal_behavior_after_lfr_error)
                                                remove(my_path_buf);

					if(is_mail_active)
                                        	sendmail(my_hostname, from_mail, mail_cmd_to, logging_buf);

					if(!lfr_error_statement)
					{
						error_code = 1;
						break;
					}

				}

				fflush(logging_fp);
			        // pclose(pp); // already closed after GPFS's mmgetstate command output parsing

			}
			
		}
		else // if(fault_flag)
		{
			fault_flag = 0;
			if(file_exists(my_path_buf))
				remove(my_path_buf);
		}

	}

	close(fd);

	const char * logging_timebuf = gettime(&time_end);
	fprintf(logging_fp, "\n\n[%s] END LOGGING\nTotal Elapsed Time: %ld seconds.\n", logging_timebuf, time_end-time_0);
	fflush(logging_fp);
	fclose(logging_fp);

	#undef is_mail_active

        return error_code;
}
