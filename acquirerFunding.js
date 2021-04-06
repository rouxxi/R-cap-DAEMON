<?php

use AcquirerReconciliation_Exception_FundingMismatchAmountException as FundingMismatchAmountException;
use AcquirerReconciliation_Exception_NotFoundException as NotFoundException;

/**
 * Rolling reserve daemon
 *
 * @category Backend
 *
 * @package Backend_Daemon
 *
 * @author Jerome Rivron <jrivron@hi-media.com>
 *
 * @copyright 2013 Hi-media
 */
class AcquirerFunding extends Backend_Pgq_Consumer
{
    const FUNDING_RECONCILED    = 'RECONCILED';
    const FUNDING_NOT_PROCESSED = 'NOT_PROCESSED';
    const FUNDING_FORCED        = 'FORCED';
    const FUNDING_MISMATCH      = 'MISMATCH';

    const BATCH_CREATED       = 'CREATED';
    const BATCH_ERROR         = 'ERROR';
    const BATCH_SUCCESS       = 'SUCCESS';
    const BATCH_RETRY         = 'RETRY';

    /**
     * @var Zend_Db_Adapter_Abstract
     */
    protected $_db;

    /**
     * @var integer
     */
    protected $_batchId;

    /**
     * @var array
     */
    protected $_fundingPaymentIds;

    /**
     * Fundings to be fetched from loadFundingPaymentId()
     *
     * @var PDOStatement
     */
    protected $_fundings;

    /**
     * Savepoint commands
     */
    const SAVEPOINT_QUERY = 'SAVEPOINT acquirer_funding_savepoint';

    const RELEASE_SAVEPOINT_QUERY = 'RELEASE SAVEPOINT acquirer_funding_savepoint';

    const ROLLBACK_TO_SAVEPOINT_QUERY = 'ROLLBACK TO SAVEPOINT acquirer_funding_savepoint';

    /**
     * @param int $batchId
     */
    public function batchStartup ($batchId)
    {
        $this->_db = $this->getDb('backend');
        $this->_batchId = $batchId;
    }

    /**
     * Process event.
     *
     * Returns one of those constants: EVENT_OK, EVENT_FAILED, EVENT_RETRY or
     * ABORT_BATCH
     *
     * @param Himedia_Pgq_Event $event
     *            Event being processed.
     *
     * @return int Return the event status.
     */
    public function processEvent (Himedia_Pgq_Event $event)
    {
        $data   = $event->getData();
        $logger = $this->getLogger();

        $logger->debug('========= Begin Process_event =========..');

        $forced = isset($data['force']) && ($data['force'] == 't' || $data['force'] === true);
        $this->_fundingPaymentIds = null;

        $logger->debug('Received event data=[' .  json_encode($data) . ']');

        if (isset($data['batchid']) && !isset($data['funding_paymentid'])) {
            $logger->debug('Current batch id : ' . $data['batchid']);
        } else if (isset($data['batchid']) && isset($data['funding_paymentid'])) {
            $logger->debug('Partial treatment of batch : ' . $data['batchid']);
            $logger->debug('Only this list of funding(s) will be process : ' . $data['funding_paymentid']);
            $this->_fundingPaymentIds = json_decode($data['funding_paymentid']);
        } else if (!isset($data['batchid']) && isset($data['funding_paymentid'])) {
            $logger->debug('Only this list of funding(s) will be process : ' . $data['funding_paymentid']);
            $logger->debug('The fundingId list can affect several batches');
            $this->_fundingPaymentIds = json_decode($data['funding_paymentid']);
            $data['batchid'] = null ;
        } else {
            $logger->error('No funding batchid or paymentid given. Abort event.');
            $event->setFailedReason('No funding batchid or paymentid given. Abort event.');
            return self::EVENT_FAILED;
        }

        $counters_type = array(self::FUNDING_RECONCILED, self::FUNDING_NOT_PROCESSED, self::FUNDING_FORCED);
        $counters = array_fill_keys($counters_type, 0);

        $retry = false;

        try {
            // init savepoint
            $this->_db->query(self::SAVEPOINT_QUERY);

            $logger->info('Loading the funding payments');
            $fundingPaymentIdLoaded = $this->loadFundingPayments($data);

            if ($fundingPaymentIdLoaded == 0) {
                $logger->warn('Fundings payment id not found');
            } else {
                $logger->info($fundingPaymentIdLoaded . ' funding payment(s) found, updating funding(s)');

                try {
                    while(($funding = $this->fetchRecord()) != null) {

                        if ($funding['funding_status'] == self::FUNDING_RECONCILED) {
                            $counters[self::FUNDING_RECONCILED] += 1;
                            $logger->info('Funding already reconciled (' . $funding['funding_paymentid'] . '), skip to next funding');
                            continue;
                        }

                        $bypassValidation = false;
                        if (in_array($funding['acquirer'], array('BCMC', 'SOFORT'))) {
                            // Bypassing validation for BCMC ...
                            $bypassValidation = true;
                        }

                        if ($bypassValidation) {
                            $logger->info('Bypass validation, don\'t check if amounts are equals between funding and remittance');
                            $this->updateAcquirerFunding($funding['funding_paymentid'], array('funding_status' => self::FUNDING_FORCED));
                        } else {
                            $logger->info('Checking if amounts matches between remittance and funding.');
                            try {
                                // @TODO Update financial operations here in a savepoint
                                // @TODO Use financial_operation to check amounts instead of a view
                                // @TODO Rollback on mismatch of amounts
                                $this->validateFundingAmounts($funding, $funding['funding_paymentid']);
                                $counters[self::FUNDING_RECONCILED] += 1;
                                $this->updateAcquirerFunding($funding['funding_paymentid'], array('funding_status' => self::FUNDING_RECONCILED));
                                $logger->info('Amounts are ok.');
                            } catch (FundingMismatchAmountException $e) {
                                $retry = true;
                                $logger->error($e->getMessage());

                                if ($forced) {
                                    $logger->warn('Batch flagged as "FORCED", reconciling the funding (' . $funding['funding_paymentid'] . ') anyway');
                                    $this->updateAcquirerFunding($funding['funding_paymentid'], array('funding_status' => self::FUNDING_FORCED));

                                    $counters[self::FUNDING_FORCED] += 1;
                                } else {
                                    $logger->warn('Funding flagged as "MISMATCH" (' . $funding['funding_paymentid'] . ')');
                                    $this->updateAcquirerFunding($funding['funding_paymentid'], array('funding_status' => self::FUNDING_MISMATCH));
                                }
                            }
                        }

                        $this->insertInFundingReconciliationQueue($funding['funding_paymentid']);
                    }

                    $this->updateFinancialOperation($data['batchid'], $this->_fundingPaymentIds);
                } catch (Exception $e) {
                    if (strpos($e->getMessage(), 'deadlock') !== false) {
                            $logger->error(
                                'Catched deadlock on financial operation update. Rolling back and settling the event as retry. Error code : ' .
                                $e->getCode() . '. Exception class : ' . get_class($e) . '.'
                            );

                            // Has to rollback to savepoint :
                            // Transaction is dead, need to rollback in order to insert in event later on.
                            $this->_db->query(self::ROLLBACK_TO_SAVEPOINT_QUERY);

                            if ($event->getRetryCount() + 1 > (int)$this->getOption('max_attempts')) {
                                $logger->debug('Reached the max attempts retry, tag the event as failed');
                                $event->setFailedReason('Deadlock detected and max retry reached.');
                                return self::EVENT_FAILED;
                            } else {
                                $logger->debug('Did not reach the max attempts retry, tag the event as retry');
                                return self::EVENT_RETRY;
                            }
                    } else {
                        throw $e;
                    }
                }

                $logger->debug('Finished process of funding payments successfully ');
            }

            if ($retry) {
                $logger->error('Trying to retry event');
                $event->setFailedReason('Some fundings could not be reconciled properly');

                $logger->debug('Check the max number of allowed attempts');

                if ($event->getRetryCount() + 1 > (int)$this->getOption('max_attempts')) {
                    $logger->debug('Reached the max attempts retry');
                    $logger->info('Updating batch(es) with status [' . self::BATCH_ERROR . ']');
                    $this->updateFileBatch($data['batchid'], $this->_fundingPaymentIds);
                    $pgqCode = self::EVENT_OK;
                } else {
                    $logger->debug('Did not reach the max attempts retry, tag the event as retry');
                    $this->updateFileBatch($data['batchid'], $this->_fundingPaymentIds);
                    $pgqCode = self::EVENT_RETRY;
                }
            } else {
                $logger->info('Updating batch [' . $data['batchid'] . '] with status [' . self::BATCH_SUCCESS . ']');
                $this->updateFileBatch($data['batchid'], $this->_fundingPaymentIds);

                $pgqCode = self::EVENT_OK;
            }

            // release savepoint
            $this->_db->query(self::RELEASE_SAVEPOINT_QUERY);
        } catch (Exception $e) {
            // rollback to savepoint
            $this->_db->query(self::ROLLBACK_TO_SAVEPOINT_QUERY);

            $logger->error($e);
            $logger->error('Updating batch(es) with status [' . self::BATCH_ERROR . ']');

            $event->setFailedReason($e->getMessage());

            try {
                $this->updateFileBatch($data['batchid'], $this->_fundingPaymentIds);
            } catch (NotFoundException $e) {
                $logger->error('Batch or Funding Id not found.');
            }

            $pgqCode = self::EVENT_FAILED;
        }

        $logger->debug('========== End process event ==========');

        return $pgqCode;
    }

    /**
     * Find funding payment id
     *
     * @param $data
     * @return string
     */
    private function loadFundingPayments(array $data)
    {
        $select = $this->_db->select();

        $select->from('fsfinance.acquirer_funding');

        if (isset($this->_fundingPaymentIds)) {
            $this->getLogger()->info('Loading custom funding_paymentid ' . json_encode($this->_fundingPaymentIds));
            $select->where("funding_paymentid IN (?)", $this->_fundingPaymentIds, Zend_Db::INT_TYPE);
        }

        if(isset($data['batchid'])) {
            $select->where('batchid = ?', $data['batchid'], Zend_Db::INT_TYPE);
        }

        $this->_fundings = $this->_db->query($select);

        $countResult = $this->_fundings->rowCount();

        if(isset($this->_fundingPaymentIds) && $countResult != count($this->_fundingPaymentIds) && !isset($data['batchid'])) {
            $this->getLogger()->error("At least one of this funding can't be found : " . implode(', ', $this->_fundingPaymentIds));
            return 0;
        }

        return $countResult;
    }

    /**
     * Insert or update funding reconciliation if funding payment id is specified
     *
     * @param integer $fundingPaymentid
     * @param array $bindings
     * @return integer
     * @throws Zend_Db_Adapter_Exception
     */
    private function updateAcquirerFunding ($fundingPaymentid = null, array $bindings = array())
    {
        if ($fundingPaymentid !== null) {
            $rows = $this->_db->update('fsfinance.acquirer_funding', $bindings, array('funding_paymentid = ?' => $fundingPaymentid));
            if ($rows !== 1) {
                throw new RuntimeException('Update failed for funding_paymentid : ' . $fundingPaymentid);
            }

            $this->getLogger()->info('[Updated : funding_reconciliation successful ' . $fundingPaymentid . ' with ' . json_encode($bindings));
        }
    }

    /**
     * Insert into funding paymentid into funding reconciliation queue
     *
     * @param $paymentid
     */
    protected function insertInFundingReconciliationQueue($paymentid)
    {
        $query = "SELECT bank_trxid FROM fsfinance.acquirer_funding WHERE funding_paymentid = ? ORDER BY 1 ASC";

        $bank_trxid = $this->_db->query($query, $paymentid)->fetchColumn();

        if ($bank_trxid !== null) {
            $this->getLogger()->warn('Bank trxid found, force reconciliation : #' . $bank_trxid);
        }

        $this->_db->insert('fsevent.funding_reconciliation', array('funding_paymentid' => $paymentid, 'bank_trxid' => $bank_trxid));

    }

    /**
     * Validate funding amounts
     *
     * Check that they match the remittance amounts
     *
     * @param array $data
     * @param $paymentid
     * @return bool
     * @throws FundingMismatchAmountException
     */
    private function validateFundingAmounts($data, $paymentid)
    {
        $logger = $this->getLogger();

        if ($data['chargeback_amount'] == 0 &&
            $data['capture_amount'] == 0 &&
            $data['chargebackreturn_amount'] == 0 &&
            $data['refund_amount'] == 0 &&
            $data['credit_amount'] == 0) {
            $logger->info('Funding without transactions');
            return true;
        }

        $query = "
            SELECT SUM(CASE WHEN optype IN ('SALE')       THEN  ABS(remittance_amount)
                            WHEN optype IN ('REF')        THEN -ABS(remittance_amount)
                            WHEN optype IN ('CREDIT')     THEN -ABS(remittance_amount)
                            WHEN optype IN ('CHGBCK_REF') THEN  ABS(remittance_amount)
                            WHEN optype IN ('CHGBCK')     THEN -ABS(remittance_amount)
                   END) AS total_gross_amount
                 , SUM(CASE WHEN optype = 'SALE' THEN remittance_amount ELSE 0 END) AS capture_amount
                 , SUM(CASE WHEN optype = 'REF' THEN ABS(remittance_amount) ELSE 0 END) AS refund_amount
                 , SUM(CASE WHEN optype = 'CREDIT' THEN ABS(remittance_amount) ELSE 0 END) AS credit_amount
                 , SUM(CASE WHEN optype = 'CHGBCK' THEN -ABS(remittance_amount) ELSE 0 END) AS chargeback_amount
                 , SUM(CASE WHEN optype = 'CHGBCK_REF' THEN ABS(remittance_amount) ELSE 0 END) AS chargebackreturn_amount
                 , COUNT (*)
              FROM fsfinance.v_funding_remittance d
             WHERE d.funding_paymentid = :paymentid";

        $remittance = $this->_db->fetchRow($query, array('paymentid' => $paymentid), Zend_Db::FETCH_ASSOC);

        $info = array(
            'data'       => $data,
            'remittance' => $remittance,
            'paymentid'  => $paymentid
        );

        if ($remittance['count'] == 0) {
            throw new FundingMismatchAmountException('Could not find any operation to match with the funding : ' . json_encode($info));
        }

        $errors = array();
        $tpl = '%s does not match between funding and remittance, got %s for funding and expecting %s in remittance.';
        if ($remittance['total_gross_amount'] != $data['total_gross_amount']) {
            # Extra check for omnipay ... add approximation because on the chargeback there is a fx that make amounts not equal...
            # All of the CBKs arrive to our dispute account which is denominated in EUR currency, so that´s why you see the CBK in EUR currency.
            # Regarding the 0,01 USD difference – it is caused by an FX difference, as the EUR amount is converted to USD.
            $bypass = false;
            $totChgbck  = $data['chargeback_amount'];
            $totSaleOp  = $data['capture_operation_count'];
            $remGross   = $remittance['total_gross_amount'];
            $funGross   = $data['total_gross_amount'];
            if ($data['acquirer'] == 'OMNIPAY' && $totChgbck > 0) {
                $logger->info('Trying to approximate omnipay amount because of chargeback');
                // Allow 2% difference on chargebacks due to fx change ...
                $difference = abs($remGross - $funGross);
                $allowed    = abs($totChgbck * 0.02);

                $logger->info('Difference between remittance and funding gross : ' . $difference);
                $logger->info('Allowed difference : ' . $allowed);

                if ($difference <= $allowed) {
                    $logger->info('Approximation of gross amount because of omnipay fx on chargeback');
                    $bypass = true;
                }

                // Float comparison with php
                // http://stackoverflow.com/questions/3148937/compare-floats-in-php
                if (bccomp($remGross - $data['chargeback_amount'], $funGross, 5) == 0) {
                    $logger->info('There is no way on EMS to identify a chargeback, bypassing the funding.');
                    $bypass = true;
                }
            }

            if ($data['acquirer'] == 'SISAL') {
                $difference = abs($remGross - $funGross);
                $allowed    = abs($totSaleOp * 0.01);

                $logger->info('Sisal difference : ' . $difference . '; allowed : ' . $allowed);

                if ($difference <= $allowed) {
                    $logger->info('Approximation of gross amount because of sisal cents bug.');
                    $bypass = true;

                    // UPDATE du gross amount et du capture amount pour sisal
                    $amounts = array(
                        'total_gross_amount' => $remGross,
                        'capture_amount'     => $remGross,
                        'total_net_amount'   => $remGross
                    );

                    $this->updateAcquirerFunding($paymentid, $amounts);
                }
            }

            if (!$bypass) {
                $message = sprintf($tpl, 'Total gross amount', $data['total_gross_amount'], $remittance['total_gross_amount']);
                $logger->error($message);
                $errors['total_gross_amount'] = $message;
            }
        }

        if ($remittance['capture_amount'] != $data['capture_amount']) {
            $message = sprintf($tpl, 'Capture amount', $data['capture_amount'], $remittance['capture_amount']);
            $logger->error($message);
            $errors['capture_amount'] = $message;
        }

        if ($remittance['refund_amount'] != $data['refund_amount']) {
            $message = sprintf($tpl, 'Refund amount', $data['refund_amount'], $remittance['refund_amount']);
            $logger->error($message);
            $errors['refund_amount'] = $message;
        }

        if ($remittance['credit_amount'] != $data['credit_amount']) {
            $message = sprintf($tpl, 'Credit amount', $data['credit_amount'], $remittance['credit_amount']);
            $logger->error($message);
            $errors['credit_amount'] = $message;
        }

        if (!empty($errors)) {
            throw new FundingMismatchAmountException('Funding did not match with remittance data ' . json_encode($errors));
        }
    }

    /**
     * @param $batchId
     * @param array $fundings
     * @throws NotFoundException
     */
    private function updateFileBatch($batchId, $fundings = array())
    {
        $bindings = array(
            ':reconciled' => self::FUNDING_RECONCILED,
            ':forced'     => self::FUNDING_FORCED
        );

        if(!empty($fundings)) {
            $where = "funding_paymentid = ANY('{" . implode(', ', $fundings) . "}'::bigint[])";
            if (isset($batchId)) {
                $bindings['batchid'] = $batchId;
                $where .= " AND batchid = :batchid";
            }

            //Current event only process special funding(s)'ID (isn't treating the entire batch)
            $query = "
                WITH cte_batches as (
                  select batchid
                    from fsfinance.acquirer_funding
                   where " . $where . "
                   GROUP BY batchid
                )";
        } else {
            $query = "
                WITH cte_batches as (
                  select batchid
                    from fsfinance.file_batch
                   where batchid = :batchid
                   GROUP BY batchid
                )";

            $bindings['batchid'] = $batchId;
        }

        $query .= "
          , cte_count AS (
              SELECT count(*) FILTER (WHERE funding_status = :reconciled) AS reconciled
                   , count(*) FILTER (WHERE funding_status = :forced) AS forced
                   , cb.batchid as batchid
                FROM cte_batches cb
                LEFT JOIN fsfinance.acquirer_funding af on cb.batchid = af.batchid
                GROUP BY cb.batchid
          )
          UPDATE fsfinance.file_batch fb
             SET operation_reconciled = cte.reconciled
               , operation_forced = cte.forced
            FROM cte_count cte
           WHERE cte.batchid = fb.batchid
       RETURNING *
               ";

        $stmt = $this->_db->query($query, $bindings);

        $this->getLogger()->debug('The following file_batch were updated: ' . json_encode($stmt->fetchAll()));

        if ($batchId && $stmt->rowCount() !== 1) {
            throw new NotFoundException('Could not update fsfinance.file_batch status.');
        }
    }

    public function fetchRecord()
    {
        return $this->_fundings->fetch();
    }

    /**
     * Update the financial_operations that have been properly funded
     *
     * @param $batchId
     * @return void
     */
    public function updateFinancialOperation($batchId, $fundingsId)
    {
        $logger = $this->getLogger();
        $param = array();
        $where = array();

        $query = "
            UPDATE fsfinance.financial_operation fo
               SET funding_paymentid = ffunding_paymentid,
                   date_funded = now()
              FROM (
              SELECT fo.opid oopid,
                     fr.optype ooptype,
                     af.date_created ddate_created,
                     af.funding_paymentid ffunding_paymentid,
                     fo.funding_paymentid as original_funding_paymentid
                FROM fsfinance.acquirer_funding af
                LEFT JOIN fsfinance.v_funding_remittance fr ON (af.funding_paymentid = fr.funding_paymentid)
                LEFT JOIN fsfinance.financial_operation fo ON (fo.opid = fr.opid)";


        if(isset($batchId)) {
            $where[] = " af.batchid = :batchid ";
            $param[':batchid'] = $batchId;
        }

        if((isset($fundingsId))) {
            $where[] = " af.funding_paymentid = ANY('{" . implode(', ', $fundingsId) . "}'::bigint[]) ";
        }

        $query .=    "
         WHERE " . implode(' AND ', $where) . "
         ) af
             WHERE opid = oopid
               AND optype = ooptype
               AND (original_funding_paymentid is null or original_funding_paymentid != fo.funding_paymentid)";

        $statement = $this->_db->query($query, $param);
        $logger->info('Updated ' . $statement->rowCount() . ' financial operations rows.');
    }
}
