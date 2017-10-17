//modif seb
package cmindicators.java.com.airbushelicopter.dbAPI;

import java.io.FileSEB; // modif
import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cmindicators.java.com.airbushelicopter.common.CMindicators_Constants;
import cmindicators.java.com.airbushelicopter.common.decodeInputFile;

public class dbConnection {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(dbConnection.class);
	private decodeInputFile decinput = new decodeInputFile();
	private Connection conn;
	private int S_int_log_level;
	private String S_str_random = null;
	private String S_str_UNIC_log_tag = null;
	private boolean S_bool_unic_instance = false;
	private boolean S_bool_Keep_output_csv = false;
	private File S_file_connect = null;
	private String S_str_DMAU_IRS = null;
	private String S_str_DMS_IRS = null;
	private String S_str_145T2_IRS = null;
	private String S_str_Compteur_anomalies = null;
	private String S_str_Label_afficher = null;
	private String S_str_Temps_vol_cumule = null;
	private String S_str_GND = null;
	private String S_str_SFL = null;
	private String S_str_Duree_totale_de_vol = null;
	private String S_str_Nb_phases = null;
	private String S_str_Date_debut = null;
	private String S_str_issue_F_IRS = null;
	private int S_int_nbr_top_rotor_H175 = 0;
	private String S_str_top_rotor_MR_H175 = null;
	private String S_str_top_rotor_TR_H175 = null;
	private int S_int_BIT_top_rotor_MR_H175 = 0;
	private int S_int_BIT_top_rotor_TR_H175 = 0;
	private int S_int_nbr_top_rotor_145T2 = 0;
	private String S_str_top_rotor_MR_145T2 = null;
	private String S_str_top_rotor_TR_145T2 = null;
	private int S_int_BIT_top_rotor_MR_145T2 = 0;
	private int S_int_BIT_top_rotor_TR_145T2 = 0;
	private String S_str_top_rotor_NR_PHW = null;
	private int S_int_BIT_top_rotor_NR_PHW = 0;
	private String S_str_top_rotor_NG1 = null;
	private int S_int_BIT_top_rotor_NG1 = 0;
	private String S_str_top_rotor_NG2 = null;
	private int S_int_BIT_top_rotor_NG2 = 0;

	private String S_str_match_indicator_csv_file_path = null;

	// -----------------------------------getter----------------------------------------------
	public String get_S_str_match_indicator_csv_file_path() {
		return S_str_match_indicator_csv_file_path;
	}

	// getter TOP H175
	public int get_S_int_nbr_top_rotor_H175() {
		return S_int_nbr_top_rotor_H175;
	}

	public String get_S_str_top_rotor_MR_H175() {
		return S_str_top_rotor_MR_H175;
	}

	public String get_S_str_top_rotor_TR_H175() {
		return S_str_top_rotor_TR_H175;
	}

	public int get_S_int_BIT_top_rotor_MR_H175() {
		return S_int_BIT_top_rotor_MR_H175;
	}

	public int get_S_int_BIT_top_rotor_TR_H175() {
		return S_int_BIT_top_rotor_TR_H175;
	}

	// getter TOP 145T2
	public int get_S_int_nbr_top_rotor_145T2() {
		return S_int_nbr_top_rotor_145T2;
	}

	public String get_S_str_top_rotor_MR_145T2() {
		return S_str_top_rotor_MR_145T2;
	}

	public String get_S_str_top_rotor_TR_145T2() {
		return S_str_top_rotor_TR_145T2;
	}

	public int get_S_int_BIT_top_rotor_MR_145T2() {
		return S_int_BIT_top_rotor_MR_145T2;
	}

	public int get_S_int_BIT_top_rotor_TR_145T2() {
		return S_int_BIT_top_rotor_TR_145T2;
	}

	public int get_S_int_BIT_top_rotor_NR_PHW() {
		return S_int_BIT_top_rotor_NR_PHW;
	}

	public int get_S_int_BIT_top_rotor_NG1() {
		return S_int_BIT_top_rotor_NG1;
	}

	public int get_S_int_BIT_top_rotor_NG2() {
		return S_int_BIT_top_rotor_NG2;
	}

	public String get_S_str_top_rotor_NR_PHW() {
		return S_str_top_rotor_NR_PHW;
	}

	public String get_S_str_top_rotor_NG1() {
		return S_str_top_rotor_NG1;
	}

	public String get_S_str_top_rotor_NG2() {
		return S_str_top_rotor_NG2;
	}

	// getter str_random
	public String get_str_random() {
		return S_str_random;
	}

	// getter str_random
	public int set_str_random() {
		return create_random_string();
	}

	public String get_str_UNIC_compil_ID() {
		return S_str_UNIC_log_tag;
	}

	// getter str_random
	public int set_str_UNIC_compil_ID(String str_session) {
		return create_str_UNIC_compil_ID(str_session);

	}

	// getter S_bool_unic_instance
	public boolean get_unic_instance() {
		return S_bool_unic_instance;
	}

	// getter S_bool_unic_instance
	public boolean get_bool_Keep_output_csv() {
		return S_bool_Keep_output_csv;
	}

	public Connection get_Connection_con() {
		return conn;
	}

	public int get_int_log_level() {
		return S_int_log_level;
	}

	public void set_int_log_level(int int_log_level) {
		S_int_log_level = int_log_level;
	}

	public void set_file_connect(File file_connect) {
		S_file_connect = file_connect;
	}

	// getter S_str_DMAU_IRS
	public String get_str_DMAU_IRS() {
		return S_str_DMAU_IRS;
	}

	// getter S_str_DMS_IRS
	public String get_str_DMS_IRS() {
		return S_str_DMS_IRS;
	}

	// getter S_str_145T2_IRS
	public String get_str_145T2_IRS() {
		return S_str_145T2_IRS;
	}

	// getter S_str_issue_F_IRS
	public String get_str_issue_F_IRS() {
		return S_str_issue_F_IRS;
	}

	public String get_str_Compteur_anomalies() {
		return S_str_Compteur_anomalies;
	}

	public String get_str_Label_afficher() {
		return S_str_Label_afficher;
	}

	public String get_str_GND() {
		return S_str_GND;
	}

	public String get_str_Temps_vol_cumule() {
		return S_str_Temps_vol_cumule;
	}

	public String get_str_SFL() {
		return S_str_SFL;
	}

	public String get_str_Duree_totale_de_vol() {
		return S_str_Duree_totale_de_vol;
	}

	public String get_str_Nb_phases() {
		return S_str_Nb_phases;
	}

	public String get_str_Date_debut() {
		return S_str_Date_debut;
	}

	// -----------------------------------connect_DB----------------------------------------------
	// Objet: Connecte la db GENERIC_HEALTH_RESULTS en fonction des paramètres
	// de connexion dans imput.xml
	// ----------------------------------------------------------------------------------------
	// Paramètres:
	// E : void
	// S : int (error)
	// Retour : int_error si une erreur est detectée.
	// ----------------------------------------------------------------------------------------
	// Historique des modifications
	// initial le 14/05/2014 SLE
	// ----------------------------------------------------------------------------------------
	/**
	 * connect_DB Public method Connect GENERIC_HEALTH_RESULTS data base
	 * according connection parameters in imput.xml
	 * 
	 * @param args
	 *            void
	 * @return 0 if connect_DB execution is correct: 0, err if not correct
	 */
	public int connect_DB(boolean bool_autocommit) {
		
		/// sle modifie pour faire des commit sous git BRANCH SLE 1
		// sle modifie pour faire des commit sous git BRANCH SLE 2
		// sle modifie pour faire des commit sous git nouvelRANCH

		decinput.set_str_UNIC_log_tag(this);
		int int_ret = CMindicators_Constants.errCode_connect_DB;
		try {
			int_ret = decinput.decodeXML_File(S_file_connect);
			if (int_ret != 0) {
				return int_ret;
			}

			int int_Length = decinput.getInputStringTab().length;
			if (int_Length == 6) {
				S_str_DMAU_IRS = CMindicators_Constants.str_DMAU_IRS;
				S_str_145T2_IRS = CMindicators_Constants.str_145T2_IRS;
				S_str_DMS_IRS = CMindicators_Constants.str_DMS_IRS;
			} else if (int_Length == 7) {
				S_str_DMAU_IRS = decinput.getInputStringTab()[6];
				S_str_145T2_IRS = CMindicators_Constants.str_145T2_IRS;
				S_str_DMS_IRS = CMindicators_Constants.str_DMS_IRS;
			} else if (int_Length == 8) {
				S_str_DMAU_IRS = decinput.getInputStringTab()[6];
				S_str_145T2_IRS = decinput.getInputStringTab()[7];
				S_str_DMS_IRS = CMindicators_Constants.str_DMS_IRS;
			} else if (int_Length == 9) {
				S_str_DMAU_IRS = decinput.getInputStringTab()[6];
				S_str_145T2_IRS = decinput.getInputStringTab()[7];
				S_str_DMS_IRS = decinput.getInputStringTab()[8];
			} else if (int_Length == 10) {
				S_str_DMAU_IRS = decinput.getInputStringTab()[6];
				S_str_145T2_IRS = decinput.getInputStringTab()[7];
				S_str_DMS_IRS = decinput.getInputStringTab()[8];
				S_str_Compteur_anomalies = decinput.getInputStringTab()[9];
				S_str_Temps_vol_cumule = decinput.getInputStringTab()[10];
				S_str_GND = decinput.getInputStringTab()[11];
				S_str_SFL = decinput.getInputStringTab()[12];
				S_str_Duree_totale_de_vol = decinput.getInputStringTab()[13];
				S_str_Nb_phases = decinput.getInputStringTab()[14];
				S_str_Date_debut = decinput.getInputStringTab()[15];
				S_str_Label_afficher = decinput.getInputStringTab()[16];
			} else if (int_Length >= 18) {
				S_str_DMAU_IRS = decinput.getInputStringTab()[6];
				S_str_145T2_IRS = decinput.getInputStringTab()[7];
				S_str_DMS_IRS = decinput.getInputStringTab()[8];
				S_str_issue_F_IRS = decinput.getInputStringTab()[9];

				S_str_Compteur_anomalies = decinput.getInputStringTab()[10];
				S_str_Temps_vol_cumule = decinput.getInputStringTab()[11];

				S_str_GND = decinput.getInputStringTab()[12];
				S_str_SFL = decinput.getInputStringTab()[13];
				S_str_Duree_totale_de_vol = decinput.getInputStringTab()[14];
				S_str_Nb_phases = decinput.getInputStringTab()[15];
				S_str_Date_debut = decinput.getInputStringTab()[16];
				S_str_Label_afficher = decinput.getInputStringTab()[17];

				S_str_match_indicator_csv_file_path = decinput
						.getInputStringTab()[18];

				S_int_nbr_top_rotor_H175 = Integer.parseInt(decinput
						.getInputStringTab()[19]);
				S_str_top_rotor_MR_H175 = decinput.getInputStringTab()[20];
				S_int_BIT_top_rotor_MR_H175 = Integer.parseInt(decinput
						.getInputStringTab()[21]);
				S_str_top_rotor_TR_H175 = decinput.getInputStringTab()[22];
				S_int_BIT_top_rotor_TR_H175 = Integer.parseInt(decinput
						.getInputStringTab()[23]);

				S_int_nbr_top_rotor_145T2 = Integer.parseInt(decinput
						.getInputStringTab()[24]);
				S_str_top_rotor_MR_145T2 = decinput.getInputStringTab()[25];
				S_int_BIT_top_rotor_MR_145T2 = Integer.parseInt(decinput
						.getInputStringTab()[26]);
				S_str_top_rotor_TR_145T2 = decinput.getInputStringTab()[27];
				S_int_BIT_top_rotor_TR_145T2 = Integer.parseInt(decinput
						.getInputStringTab()[28]);
				S_str_top_rotor_NR_PHW = decinput.getInputStringTab()[29];
				S_int_BIT_top_rotor_NR_PHW = Integer.parseInt(decinput
						.getInputStringTab()[30]);
				S_str_top_rotor_NG1 = decinput.getInputStringTab()[31];
				S_int_BIT_top_rotor_NG1 = Integer.parseInt(decinput
						.getInputStringTab()[32]);
				S_str_top_rotor_NG2 = decinput.getInputStringTab()[33];
				S_int_BIT_top_rotor_NG2 = Integer.parseInt(decinput
						.getInputStringTab()[34]);
			}

			String str_ConnectString = decinput.getInputStringTab()[0];

			String str_User = decinput.getInputStringTab()[2];
			String str_PassWord = decinput.getInputStringTab()[3];
			String str__unic_instance = decinput.getInputStringTab()[4];
			S_bool_unic_instance = (str__unic_instance.equals("true"));

			String str_Keep_output_csv = decinput.getInputStringTab()[5];
			S_bool_Keep_output_csv = (str_Keep_output_csv.equals("true"));

			LOGGER.info(S_str_UNIC_log_tag + "Connect Data Base");
			int_ret = dbConnect(str_ConnectString, str_User, str_PassWord,
					bool_autocommit);

			if (int_ret == 0) {
				LOGGER.info(S_str_UNIC_log_tag + "DB connected");
			} else {
				LOGGER.error(S_str_UNIC_log_tag + "Error in DB connection");
			}
		} catch (Exception e) {

			LOGGER.error(S_str_UNIC_log_tag
					+ "The following error occurred during "
					+ "the command-line connect_DB: ", e);
		}
		return int_ret;
	}

	// -----------------------------------close_DB----------------------------------------------
	// Objet: Ferme la db GENERIC_HEALTH_RESULTS en fonction de l'argument
	// bool_dB_OK
	// bool_dB_OK -> bonne exécution des procédures stockées
	// ----------------------------------------------------------------------------------------
	// Paramètres:
	// E : boolean bool_dB_OK
	// S : boolean
	// Retour : false si une erreur est detectée.
	// ----------------------------------------------------------------------------------------
	// Historique des modifications
	// initial le 14/05/2014 SLE
	// ----------------------------------------------------------------------------------------
	/**
	 * close_DB Public method Commit then Close GENERIC_HEALTH_RESULTS data base
	 * according bool_dB_OK
	 * 
	 * @param args
	 *            boolean bool_dB_OK
	 * @return true if close_DB execution is correct
	 */
	public int close_DB(boolean bool_dB_OK) {

		int int_ret = CMindicators_Constants.errCode_close_DB;

		if (bool_dB_OK) {
			LOGGER.info(S_str_UNIC_log_tag
					+ "Close Data Base after good stocked "
					+ "procedures execution");
			try {

				LOGGER.info(S_str_UNIC_log_tag + "Close Data Base");
				conn.close();
				int_ret = 0;
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.error(S_str_UNIC_log_tag
						+ "The following error occurred during "
						+ "the command-line close_DB  ", e);
			}
		} else {
			LOGGER.error(S_str_UNIC_log_tag + "Close Data Base after bad "
					+ "stocked procedures " + "execution");
			try {
				LOGGER.info(S_str_UNIC_log_tag + "Close Data Base");
				conn.close();
				int_ret = 0;
			} catch (Exception e) {
				LOGGER.error(S_str_UNIC_log_tag
						+ "The following error occurred during "
						+ "the command-line close_DB", e);
			}
		}

		return int_ret;
	}

	public int close_DB_commit(boolean bool_dB_OK) {

		int int_ret = CMindicators_Constants.errCode_close_DB;

		if (bool_dB_OK) {
			LOGGER.info(S_str_UNIC_log_tag
					+ "Close Data Base after good stocked "
					+ "procedures execution");
			try {
				LOGGER.info(S_str_UNIC_log_tag + "commit");
				conn.commit();
				LOGGER.info(S_str_UNIC_log_tag + "Close Data Base");
				conn.close();
				int_ret = 0;
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.error(S_str_UNIC_log_tag
						+ "The following error occurred during "
						+ "the command-line close_DB good exec sp: ", e);
			}
		} else {
			LOGGER.error(S_str_UNIC_log_tag + "Close Data Base after bad "
					+ "stocked procedures " + "execution");
			try {
				LOGGER.error(S_str_UNIC_log_tag + "rollback");
				conn.rollback();
				LOGGER.error(S_str_UNIC_log_tag + "Close Data Base");
				conn.close();
				int_ret = 0;
			} catch (Exception e) {
				LOGGER.error(S_str_UNIC_log_tag
						+ "The following error occurred during "
						+ "the command-line close_DB", e);
			}
		}

		return int_ret;
	}

	// ----------------------------dbConnect-------------------------------------------
	// Objet: Connecte la db GENERIC_HEALTH_RESULTS en fonction des paramètres
	// de connexion en argument
	// ----------------------------------------------------------------------------------------
	// Paramètres:
	// E : String db_connect_string
	// String db_userid
	// String db_password
	// S : boolean
	// Retour : false si une erreur est detectée.
	// ----------------------------------------------------------------------------------------
	// Historique des modifications
	// initial le 14/05/2014 SLE
	// ----------------------------------------------------------------------------------------
	/**
	 * dbConnect PRIVATE method Connect GENERIC_HEALTH_RESULTS data base
	 * according connection parameters in argument
	 * 
	 * @param args
	 *            String db_connect_string String db_userid String db_password
	 * @return true if dbConnect execution is correct
	 */
	private int dbConnect(String db_connect_string, String db_userid,
			String db_password, boolean bool_autocommit) {
		int int_ret = CMindicators_Constants.errCode_db_Connection;
		try {

			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			conn = DriverManager.getConnection(db_connect_string, db_userid,
					db_password);
			conn.setAutoCommit(bool_autocommit);
			int_ret = 0;
		} catch (Exception e) {

			LOGGER.error(
					S_str_UNIC_log_tag
							+ "The following error occurred during the command-line dbConnect: ",
					e);
		}
		return int_ret;
	}

	private int create_random_string() {
		int int_ret = CMindicators_Constants.errCode_create_random_string;
		try {
			SecureRandom random = new SecureRandom();
			String str_random = new BigInteger(10, random).toString(16);
			if (S_int_log_level == 1) {
				LOGGER.info(S_str_UNIC_log_tag + "random String = "
						+ str_random);
			}
			S_str_random = str_random;
		} catch (Exception e) {
			LOGGER.error(S_str_UNIC_log_tag
					+ "The following error occurred during "
					+ "   the command-line create_random_string: ", e);
		}
		return int_ret;
	}

	private int create_str_UNIC_compil_ID(String str_session) {
		int int_ret = CMindicators_Constants.errCode_create_str_UNIC_compil_ID;
		try {
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date();
			String str_now = dateFormat.format(date);
			S_str_UNIC_log_tag = str_now + " SESS: " + str_session + " ";
			int_ret = 0;

		} catch (Exception e) {
			LOGGER.error(S_str_UNIC_log_tag
					+ "The following error occurred during "
					+ "   the command-line create_str_UNIC_compil_ID: ", e);
		}
		return int_ret;
	}

}
