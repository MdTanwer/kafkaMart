package com.kafkamart.inventory;

import jakarta.transaction.Transactional;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/api/inventory")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "inventory", description = "Stock lookup and restock")
@Transactional
public class InventoryResource {
    public record StockView(String sku, int quantity) {
        static StockView from(Stock stock) {
            return new StockView(stock.sku, stock.quantity);
        }
    }

    @GET
    @Path("/{sku}")
    @Operation(summary = "Current on-hand quantity for a SKU")
    public StockView get(@PathParam("sku") @NotBlank String sku) {
        return Stock.findBySku(sku)
                .map(StockView::from)
                .orElseThrow(() -> new NotFoundException("unknown sku " + sku));
    }

    @POST
    @Path("/{sku}/restock")
    @Operation(summary = "Add quantity for a SKU (creates the row if needed)")
    public StockView restock(
            @PathParam("sku") @NotBlank String sku, @QueryParam("qty") @Min(1) int qty) {
        return StockView.from(Stock.restock(sku, qty));
    }
}
