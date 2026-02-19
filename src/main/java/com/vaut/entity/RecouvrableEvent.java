package com.vaut.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "RECOUVRABLE_EVENTS", indexes = {
    @Index(name = "idx_recouv_id_passage", columnList = "id_passage", unique = true)
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RecouvrableEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "recouvrable_seq")
    @SequenceGenerator(name = "recouvrable_seq", sequenceName = "RECOUVRABLE_SEQ", allocationSize = 50)
    private Long id;

    @Column(name = "id_passage", nullable = false, unique = true)
    private String idPassage;

    // Tarification - Trajet - Entree
    @Column(name = "entree_gare")
    private String entreeGare;

    @Column(name = "entree_voie")
    private String entreeVoie;

    @Column(name = "entree_dhm")
    private LocalDateTime entreeDhm;

    // Tarification - Trajet - Sortie
    @Column(name = "sortie_gare")
    private String sortieGare;

    @Column(name = "sortie_voie")
    private String sortieVoie;
    
    // Tarification - Trajet
    @Column(name = "trajet_kilometrage")
    private Double trajetKilometrage;

    // Tarification - Classe Tarifaire
    @Column(name = "classe_tarifaire_acquisition")
    private String classeTarifaireAcquisition;

    @Column(name = "classe_tarifaire_classe")
    private String classeTarifaireClasse;

    // Tarification - Prix
    @Column(name = "prix_montant_ht")
    private Double prixMontantHT;

    @Column(name = "prix_taux_tva")
    private Double prixTauxTVA;

    @Column(name = "prix_devise")
    private String prixDevise;

    // Paiement - Retenus (Assuming first element extraction)
    @Column(name = "paiement_montant")
    private Double paiementMontant;

    @Column(name = "paiement_devise")
    private String paiementDevise;

    @Column(name = "paiement_type")
    private String paiementType;

    @Column(name = "paiement_acquisition")
    private String paiementAcquisition;

    @Column(name = "systeme")
    private String systeme;

    @Column(name = "etat")
    private String etat;

    @Column(name = "dhm")
    private LocalDateTime dhm;

    @Column(name = "localisation_gare")
    private String localisationGare;

    @Column(name = "equipement_numero")
    private String equipementNumero;

    @Column(name = "equipement_type")
    private String equipementType;

    @Column(name = "kafka_topic")
    private String kafkaTopic;

    @Column(name = "kafka_partition")
    private Integer kafkaPartition;

    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    @Lob
    @Column(name = "payload", columnDefinition = "CLOB")
    private String payload;

}
