package com.vaut.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * JPA Entity representing a specific business event (Recouvrable).
 * This entity captures detailed domain-specific fields from the Kafka payload.
 */
@Entity
@Table(name = "RECOUVRABLE_EVENTS", indexes = {
    @Index(name = "idx_recouv_id_passage", columnList = "id_passage", unique = true)
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RecouvrableEvent {

    /** Technical primary key. */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "recouvrable_seq")
    @SequenceGenerator(name = "recouvrable_seq", sequenceName = "RECOUVRABLE_SEQ", allocationSize = 50)
    private Long id;

    /** Unique passage identifier. */
    @Column(name = "id_passage", nullable = false, unique = true)
    private String idPassage;

    // Tarification - Trajet - Entree
    /** Gare of entry. */
    @Column(name = "entree_gare")
    private String entreeGare;

    /** Lane of entry. */
    @Column(name = "entree_voie")
    private String entreeVoie;

    /** Timestamp of entry. */
    @Column(name = "entree_dhm")
    private LocalDateTime entreeDhm;

    // Tarification - Trajet - Sortie
    /** Gare of exit. */
    @Column(name = "sortie_gare")
    private String sortieGare;

    /** Lane of exit. */
    @Column(name = "sortie_voie")
    private String sortieVoie;
    
    // Tarification - Trajet
    /** Distance traveled in kilometers. */
    @Column(name = "trajet_kilometrage")
    private Double trajetKilometrage;

    // Tarification - Classe Tarifaire
    /** Method used to acquire the tariff class. */
    @Column(name = "classe_tarifaire_acquisition")
    private String classeTarifaireAcquisition;

    /** The tariff class of the vehicle. */
    @Column(name = "classe_tarifaire_classe")
    private String classeTarifaireClasse;

    // Tarification - Prix
    /** Amount excluding tax. */
    @Column(name = "prix_montant_ht")
    private Double prixMontantHT;

    /** VAT rate applied. */
    @Column(name = "prix_taux_tva")
    private Double prixTauxTVA;

    /** Currency code for the price. */
    @Column(name = "prix_devise")
    private String prixDevise;

    // Paiement - Retenus (Assuming first element extraction)
    /** Total amount paid. */
    @Column(name = "paiement_montant")
    private Double paiementMontant;

    /** Currency code for the payment. */
    @Column(name = "paiement_devise")
    private String paiementDevise;

    /** Method of payment. */
    @Column(name = "paiement_type")
    private String paiementType;

    /** Acquisition method for the payment. */
    @Column(name = "paiement_acquisition")
    private String paiementAcquisition;

    /** Name of the source system. */
    @Column(name = "systeme")
    private String systeme;

    /** Current state of the event. */
    @Column(name = "etat")
    private String etat;

    /** Timestamp of the event occurrence. */
    @Column(name = "dhm")
    private LocalDateTime dhm;

    /** Localization gare. */
    @Column(name = "localisation_gare")
    private String localisationGare;

    /** Unique equipment number. */
    @Column(name = "equipement_numero")
    private String equipementNumero;

    /** Type of equipment used. */
    @Column(name = "equipement_type")
    private String equipementType;

    /** Source Kafka topic. */
    @Column(name = "kafka_topic")
    private String kafkaTopic;

    /** Kafka partition. */
    @Column(name = "kafka_partition")
    private Integer kafkaPartition;

    /** Kafka offset. */
    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    /** The full raw JSON payload. */
    @Lob
    @Column(name = "payload", columnDefinition = "CLOB")
    private String payload;

}
