//daemon est constituer d'une classe englobant toute les méthodes
class FundingReconciliation extends Backend_Pgq_Consumer {

    1/ Définir une varialble

const ARRAY_KEY_ACQUIRER = 'acquirer';

//La méthode processEvent() va déroulet le processus du daemon et appeller toute les méthodes nécessaires aux étapes
public function processEvent(Himedia_Pgq_Event $event) {

    1 / Définit les variables nécessaire à la récupération
    $logger = $this -> getLogger();
    $evstatus = self:: EVENT_OK;
    $paymentid = $event -> get('funding_paymentid');
    $banktrxid = $event -> get('bank_trxid');
    $force = $event -> get('force') == "t";
    $db = $this -> getDb('backend');
    $paymentids = array();

    2/ Check si le acquirerFunding et le acquirerBanckAccount ont bien envoyé leur infos

    if ($paymentid && $banktrxid) non vide
    //Fin event!
        return self::EVENT_FAILED;
    else
    //remplit le tableau $paymentids par les info dans $paymentid
        $paymentids[] = $paymentid;

    //A quoi ca sert ?
    $logger->info(sprintf('Flagging payment %d as reconciled, bank_trxid %d', $paymentid, $banktrxid));

    
       => Check si $banktrxid ou $paymentid est vide

            si oui $banktrxid vide => lance la méthode findMissingId($paymentid, 'funding')

            // méthode findMissingId($paymentid, 'funding') {
            //     1/interroge la table false.finance.bank_transaction et récupère $banktrxid présent dans une collonne
            // }
            si oui $paymentid vide => lance la méthode findMissingId($banktrxid, 'bank');

            // méthode findMissingId($banktrxid, 'bank'); {
            //     1/interroge la table false.finance.bank_transaction et récupère $paymentid présent dans une collonne
            // }
    

        => Check si le $paymentid récupéré contient quelques chose

            si $paymentid est vide => lance la méthode : throw new NotFoundException('Paymentid(s) not found for bank transaction : ' . $banktrxid)

        //     méthode NotFoundException() {
        //         1/Renvoi un code 404 + un petit message dans la table fsbackend => pgq.failed_queue
        // }

        =>  Parcour chaque ligne du tableau $paymentids

            1/ForEach ligne => renomme la variable  $evstatus avec la méthode processPaymentId($event, $banktrxid, $paymentid);

            // méthode processPaymentId($event, $banktrxid, $paymentid); {
            //     1/ Déclare une variable $evstatus en self::EVENT_OK
            //     2/ Requete l'ensemble des info et les stocke dans la variable $filebatch
            //          => Si le filebatch est falsy Appelle la méthode InvalidArgumentException
            //     3/ Lance la méthode insertInAcquirerCollectQueue
            //          ==> Requete la base de donnée fsfinance.v_funding_remittance et enregistre tout dans la variable $stmt
            //              ==> Si $stmt est vide
            //                      ==> msg d'erreur
            //              ==> Si remplit
            //                      ==> continue normalement
            //          ==> while (($row = $stmt->fetch()) !== false) {
            //            $this->getLogger()->info('Inserting ' . json_encode($row) . ' into collect queue');
            //         }
            //      4/ lance la méthode updateFinancialOperation($banktrxid, $paymentid)
            //          ==> requête avec un UPDATE la table fsfinance.financial_operation dans le backend
            //              ==> Si $stmt est vide
            //                      ==> msg d'erreur
            //              ==> Si remplit
            //                      ==> continue normalement
            //          ==> while (($row = $stmt->fetch()) !== false) {
            //            $this->getLogger()->info('Inserting ' . json_encode($row) . ' into collect queue');
            //         }
            //      
            //      5/ Lance la méthode flagFundingAsReconciled($paymentid, $banktrxid)
            //          ==> Crée un tableau avec les information suivante informant la réconciliation du funding en TRUE
            //                    'reconciled_bank'      => true,
            //                    'date_reconciled_bank' => 'NOW()',
            //                    'bank_trxid'           => $banktrxid
            //          ==> Requête la DB pour update la table fs.finance.acquirer.funding dans la colonne funding_paymentid
            //          ==> Supprime la ligne dans la table dsfinance.funding.reconciliation_pending correspondant à $paymentid
            //          ==> envoi un msg que le funding à été réconcilié
            //
            // }
            2/Si  $evstatus !== self::EVENT_OK => informe le logger du soucis

        => Si $evstatus === self::EVENT_OK ===> lance la méthode flagBankTransactionAsReconciled($banktrxid)

            // méthode flagBankTransactionAsReconciled($banktrxid)
            //      1/ requete la table fsfinance.bank_transaction pour update la colonne reconcilied en TRUE 
            //      2/ informe le logger que l'opération a été flag en réconcilied
            
        => return $evstatus
}
}